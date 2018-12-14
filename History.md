
3.3.0 / 2018-12-14
==================

**features**
  * [[`9acff9b`](http://github.com/ali-sdk/ali-ons/commit/9acff9b91c0325ed88ad634ae15161f62dceb574)] - feat: support consume back (#56) (Hongcai Deng <<admin@dhchouse.com>>)

3.2.2 / 2018-11-20
==================

**fixes**
  * [[`39e3782`](http://github.com/ali-sdk/ali-ons/commit/39e37827c8eac0c16e1fb5ecd64792da331673f3)] - fix: parse date format (#53) (Hongcai Deng <<admin@dhchouse.com>>)

**others**
  * [[`e2dc377`](http://github.com/ali-sdk/ali-ons/commit/e2dc377babb3a420e2dfc99a33bf6118979825ff)] - test: fix ci (#54) (zōng yǔ <<gxcsoccer@users.noreply.github.com>>)

3.2.1 / 2018-10-18
==================

**fixes**
  * [[`d6b6fc6`](http://github.com/ali-sdk/ali-ons/commit/d6b6fc61d049c5925c65913c299d0b423573d4b6)] - fix: accessKeyID => accessKeyId (#50) (fengmk2 <<fengmk2@gmail.com>>)

3.2.0 / 2018-10-17
==================

**features**
  * [[`8ec58c8`](http://github.com/ali-sdk/ali-ons/commit/8ec58c8f773633a4f1bb0341306d89afda1972e5)] - feat: unify aliyun keys to accessKeyID and accessKeySecret (fengmk2 <<fengmk2@gmail.com>>)

**others**
  * [[`9ecd381`](http://github.com/ali-sdk/ali-ons/commit/9ecd381f7079d7be6f2551a6baabfc51cdefde46)] - f (fengmk2 <<fengmk2@gmail.com>>)
  * [[`742f269`](http://github.com/ali-sdk/ali-ons/commit/742f269534c2e30f0772663ca3690ad000d9c75e)] - f (fengmk2 <<fengmk2@gmail.com>>)

3.1.0 / 2018-09-14
==================

**others**
  * [[`d1de082`](http://github.com/ali-sdk/ali-ons/commit/d1de0823fcc860ae12133a0026ddcfe2105db82d)] - name-server-fault-tolerance (wujia <<geoff.j.wu@gmail.com>>)

3.0.0 / 2018-07-03
==================

**features**
  * [[`5624319`](http://github.com/ali-sdk/ali-ons/commit/56243195ad838b88932be8b5e5c1f2f6a2eb3d4f)] - feat: suppory async (ngot <<zhuanghengfei@gmail.com>>)

**others**
  * [[`4b84644`](http://github.com/ali-sdk/ali-ons/commit/4b846446ceec7f175a363ef3dd011f67dc2dcaea)] - test: update ci command (Hongcai Deng <<admin@dhchouse.com>>)
  * [[`ebb7942`](http://github.com/ali-sdk/ali-ons/commit/ebb7942269662843357ce4070a19628c6d51fe88)] - test: trigger ci (Hongcai Deng <<admin@dhchouse.com>>)
  * [[`79b56c8`](http://github.com/ali-sdk/ali-ons/commit/79b56c86f03a957cad4442e52ddae8e49389ab57)] - breaking: async (Hongcai Deng <<admin@dhchouse.com>>)
  * [[`1ee4785`](http://github.com/ali-sdk/ali-ons/commit/1ee47857f4846a4d70b33b60234972651d1c37d3)] - f (ngot <<zhuanghengfei@gmail.com>>)
  * [[`e0a7a77`](http://github.com/ali-sdk/ali-ons/commit/e0a7a77e0830a04c020c7a2762f32a2f210ef426)] - f (ngot <<zhuanghengfei@gmail.com>>)
  * [[`519e164`](http://github.com/ali-sdk/ali-ons/commit/519e1641e224deec08504ccf60ca4fa05e01788b)] - f (ngot <<zhuanghengfei@gmail.com>>)
  * [[`1de2b1a`](http://github.com/ali-sdk/ali-ons/commit/1de2b1a6cdc46fd19faeceda50d89c2f2928144d)] - doc: updaet readme (ngot <<zhuanghengfei@gmail.com>>)

2.0.4 / 2018-01-10
==================

**fixes**
  * [[`6f78013`](http://github.com/ali-sdk/ali-ons/commit/6f780131b713465827588ac3ee866b9e2b2bd2ae)] - fix: fix invokeOneWay issue (gxcsoccer <<gxcsoccer@126.com>>)

**others**
  * [[`46d1bfe`](http://github.com/ali-sdk/ali-ons/commit/46d1bfe4cd5af83aa814d2a6dfd490efde5172bc)] - chore: release 2.0.3 (gxcsoccer <<gxcsoccer@126.com>>),

2.0.3 / 2017-11-11
==================

  * fix: memory leak may occurred cause by Promise.race
  * doc: fix consumer and producer initialize issue

2.0.2 / 2017-10-09
==================

  * fix: persist consumer offset issue

2.0.1 / 2017-09-29
==================

  * fix: support subscribe before ready

2.0.0 / 2017-09-29
==================

  * feat: add local memory store
  * refactor: new consumer api
  * fix: consumer offset update issue

1.0.0 / 2017-03-14
==================

  * feat: implement ali-ons base on rocketmq #1
