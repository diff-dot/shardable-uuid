import { RedisHostOptions } from '@diff./redis-client';
import { expect } from 'chai';
import { ShardableUUID } from '../../src/ShardableUUID';

const testType = 1;
const testShard = 0;

const redisOptions: RedisHostOptions = {
  type: 'standalone',
  connectionKey: 'session',
  host: '127.0.0.1',
  port: 6379
};

describe('shardable-uuid', async () => {
  before(async () => {
    const uuid = new ShardableUUID({ redisOptions, shardingHint: () => testShard });
    await uuid.resetSeq(testType, testShard);
  });

  it('uuid 에서 발급시 지정한 정보가 잘 추출되는지 확인', async () => {
    const suuid = new ShardableUUID({ redisOptions });
    for (let i = 0; i < 1024; i++) {
      const { uuid, shard, sec, msec, seq } = await suuid.generate(1);
      const parsed = suuid.parse(uuid);

      expect(testType).to.be.eq(parsed.type);
      expect(shard).to.be.eq(parsed.shard);
      expect(sec).to.be.eq(parsed.sec);
      expect(msec).to.be.eq(parsed.msec);
      expect(seq).to.be.eq(parsed.seq);
    }
  });
});
