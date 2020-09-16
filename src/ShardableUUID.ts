import { RedisClient, RedisClusterOptions, RedisHostOptions } from '@diff./redis-client';

/**
 * @todo 성능 개선을 목적으로 네이티브 모듈로 변경필요
 */
export class ShardableUUID {
  private static readonly DATA_BIT = 62;
  private static readonly DATA_HEAD_BIT = 11;
  private static readonly SHARD_BIT = 10;
  private static readonly SHARD_SLOT = 1024; // 10bit, 0 ~ 1023, 샤드 슬롯 개수
  private static readonly MAX_SEQ = 127; // 7bit, 0~127
  private static readonly MAX_TYPE = 1023; // 10bit, 0 ~ 1023, 사용할 수 있는 최대 타입 개수

  private readonly redisOptions: RedisClusterOptions | RedisHostOptions;
  private readonly shardingHint: () => number;

  constructor(args: { redisOptions: RedisClusterOptions | RedisHostOptions; shardingHint?: () => number }) {
    this.redisOptions = args.redisOptions;
    this.shardingHint = args.shardingHint
      ? args.shardingHint
      : () => {
          return Math.floor(Math.random() * ShardableUUID.SHARD_SLOT);
        };
  }

  public async generate(type: number): Promise<{ uuid: string; shard: number; sec: number; msec: number; seq: number }> {
    if (type > ShardableUUID.MAX_TYPE) throw new RangeError(`Type must be less than ${ShardableUUID.MAX_TYPE}`);

    const shard = this.shardingHint() % ShardableUUID.SHARD_SLOT;
    const seq = await this.seq(type, shard);
    const stamp = this.stamp();

    /*
     * 데이터 비트셋 구성
     * - wall 1bit
     * - msec 10bit(~1023, 실 데이터는 0~999까지 존재)
     * - sec 34bit (~2514년5월30일의 타임 스템프까지 저장 가능, 이후에는 중복 가능성 발생)
     * - type : 10bit(~1023)
     * - seq : 7bit(~127)
     * - 총 62bit
     *
     * |*********|*********|*********|*********|*********|*********|**
     * |!   msec  |                sec              |   type  | seq  |
     * |*********|*********|*********|*********|*********|*********|**/
    let dataBits = BigInt(1);
    dataBits = (dataBits << BigInt(10)) | BigInt(stamp.msec); // msec
    dataBits = (dataBits << BigInt(34)) | BigInt(stamp.sec); // sec
    dataBits = (dataBits << BigInt(10)) | BigInt(type); // type
    dataBits = (dataBits << BigInt(7)) | BigInt(seq); // seq

    const uuid = this.encodeBase64UrlSafe(this.mixbit(dataBits, shard));

    return {
      uuid,
      shard,
      sec: stamp.sec,
      msec: stamp.msec,
      seq
    };
  }

  /**
   * BigInt 또는 Base64 문자열로 구성된 UUID 를 파싱
   */
  public parse(uuid: bigint | string): { type: number; shard: number; sec: number; msec: number; seq: number } {
    let source: bigint;
    if (typeof uuid === 'bigint') {
      source = BigInt(uuid);
    } else {
      source = this.decodeBase64UrlSafe(uuid);
    }

    let { data: buffer, shard } = this.unmixbit(source);

    const seq = Number(buffer & BigInt(127));
    buffer = buffer >> BigInt(7);

    const type = Number(buffer & BigInt(1023));
    buffer = buffer >> BigInt(10);

    const sec = Number(buffer & BigInt(17179869183));
    buffer = buffer >> BigInt(34);

    const msec = Number(buffer & BigInt(1023));

    return {
      type,
      shard,
      seq,
      msec,
      sec
    };
  }

  /**
   * UUID 연속 발행시 패턴이 노출될 수 있는 위험을 줄이기 위해
   * 데이터 비트셋에 5bit 간격으로 샤드 비트(1bit)를 삽입하여 난수화
   *
   * @param dataBits UUID 정보가 포함된 데이터 비트셋
   * @param shard 샤드 번호
   */
  private mixbit(dataBits: bigint, shard: number): bigint {
    let buffer = dataBits >> BigInt(ShardableUUID.DATA_BIT - ShardableUUID.DATA_HEAD_BIT);
    for (let i = 1; i <= 10; i++) {
      // 데이터 비트의 앞쪽부터 5 bit 추가
      buffer = (buffer << BigInt(5)) | ((dataBits >> BigInt(ShardableUUID.DATA_BIT - ShardableUUID.DATA_HEAD_BIT - i * 5)) & BigInt(31));
      // 샤드 비트의 앞쪽부터 5 bit 추가
      buffer = (buffer << BigInt(1)) | ((BigInt(shard) >> BigInt(ShardableUUID.SHARD_BIT - i)) & BigInt(1));
    }
    // 남은 데이터 비트(1bit) 추가
    buffer = (buffer << BigInt(1)) | (dataBits & BigInt(1));
    return buffer;
  }

  private unmixbit(uuid: bigint): { data: bigint; shard: number } {
    const mixedDataBit = ShardableUUID.DATA_BIT + ShardableUUID.SHARD_BIT;
    let dataBuffer = BigInt(uuid) >> BigInt(mixedDataBit - ShardableUUID.DATA_HEAD_BIT);
    let shardBuffer = BigInt(0);
    for (let i = 1; i <= 10; i++) {
      const dataChunkAndShardBit = (uuid >> BigInt(mixedDataBit - ShardableUUID.DATA_HEAD_BIT - i * 6)) & BigInt(63);
      dataBuffer = (dataBuffer << BigInt(5)) | (dataChunkAndShardBit >> BigInt(1));
      shardBuffer = (shardBuffer << BigInt(1)) | (dataChunkAndShardBit & BigInt(1));
    }
    dataBuffer = (dataBuffer << BigInt(1)) | (uuid & BigInt(1));

    return {
      data: dataBuffer,
      shard: Number(shardBuffer)
    };
  }

  /**
   * GENESIS를 이후 몇초가 경과하였는지를 반환
   */
  private stamp(): { msec: number; sec: number } {
    const now = new Date();
    return {
      msec: now.getMilliseconds(),
      sec: Math.floor(now.getTime() / 1000)
    };
  }

  private async seq(type: number, shard: number): Promise<number> {
    const redis = RedisClient.client(this.redisOptions);
    const script = `
    local seq = tonumber(redis.call('get', KEYS[1]))

    if not seq then seq = 0
    elseif seq >= tonumber(ARGV[1]) then seq = 0
    else seq = seq + 1
    end

    redis.call('set', KEYS[1], seq)
    return seq;
    `;

    return await redis.eval(script, 1, this.seqKey(type, shard), ShardableUUID.MAX_SEQ);
  }

  public async resetSeq(type: number, shard: number): Promise<void> {
    const redis = RedisClient.client(this.redisOptions);
    redis.del(this.seqKey(type, shard));
  }

  private seqKey(type: number, shard: number): string {
    return `uuid:${type}:${shard}`;
  }

  private encodeBase64UrlSafe(uuid: bigint): string {
    return Buffer.from(uuid.toString(16), 'hex')
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '.');
  }

  private decodeBase64UrlSafe(base64Uuid: string): bigint {
    return BigInt(
      '0x' +
        Buffer.from(
          base64Uuid
            .replace(/-/g, '+')
            .replace(/_/g, '/')
            .replace(/\./g, '='),
          'base64'
        ).toString('hex')
    );
  }

  /**
   *
   */
}
