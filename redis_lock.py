#!/usr/bin/env python
# -*- encoding: utf8 -*-
__author__ = 'roverll'
import redis
import time
import logging
import threading


class IllegalLockStateException(BaseException):
    pass


class RedisLock:
    """
    利用redis的一个自旋锁，分布式, 同一个进程内可重入，抢占式
    锁有超时时间,超时后锁失效,构造锁的时候可以传入,单位为秒
    自旋有一个lock_gap 减少对cpu redis的压力
    lock unlock 都可能发生异常（由于redis是分布式的原因）所以客户端需要考虑锁异常
    """

    # 0代表无限尝试，阻塞
    default_try_times = 0

    def __init__(self, host="localhost", port="6379", _lock_key=None, _redis_client=None, lock_expire_time=60,
                 _lock_gap=50):
        """

        :param host:
        :param port:
        :param _lock_key: 锁的特征，标识一个锁
        :param _redis_client:可以直接传入redis 客户端，否则用 host 和 port
        :param lock_expire_time: 锁有效时间，单位：s， 默认60
        :param _lock_gap: 自旋cpu休息间隙，单位 ms，默认50
        :return:
        """
        assert _lock_key is not None or _lock_gap is not None
        assert _redis_client is not None or (host is not None and port is not None)
        self.logger_ = logging.getLogger(self.__class__.__name__)
        if _redis_client:
            self.redis_client = _redis_client
        else:
            self.redis_client = redis.StrictRedis(host, port)
        self.lock_key = _lock_key
        # self.assure_lock_key 用于redis做一个乐观锁，锁失败后放弃
        # 用于保证锁失效后只有一个执行单元会删除锁，以防如下情况，A B C D 4 个执行单元同时进入锁，都发现锁超时
        # A单元 先删除锁，自己再重新竞锁，且得到锁，B 才到删除锁的地方，把A 单元的加的锁给删除了，BCD 单元重新竞锁，有一个成功，导致非法进入
        self.assure_lock_key = self.lock_key + "_assure"
        self.lock_expire = lock_expire_time
        self.lock_gap = _lock_gap / 1000.0
        self.lock_local = threading.local()

    def is_locked(self):
        try:
            self.lock_local.status
        except AttributeError:
            self.lock_local.status = 0
        return self.lock_local.status > 0

    def __add_lock(self):
        self.lock_local.status += 1

    def __exit_lock(self):
        self.lock_local.status -= 1

    def lock(self):
        return self.__lock()

    def __lock(self, try_times=default_try_times):
        """
        自旋锁实现，若果 try_times 为0 表示永远自旋
        :param try_times: 默认0
        :return:
        """
        assert try_times >= 0 and isinstance(try_times, int)
        if self.is_locked():
            self.__add_lock()
            return True
        lock_try_times = 0
        max_try_times = try_times
        flag = False
        while not flag:
            now = time.time()
            flag = self.redis_client.setnx(self.lock_key, now)
            if flag:
                self.__add_lock()
                break
            if max_try_times != 0 and lock_try_times > max_try_times:
                self.logger_.info("try max times " + str(lock_try_times))
                break
            lock_time_stamp = self.redis_client.get(self.assure_lock_key)
            lock_time = self.redis_client.get(self.lock_key)
            if lock_time is None:
                self.logger_.debug("lock time is none then retry")
                continue
            time_r = now - float(lock_time)
            if time_r > self.lock_expire:
                self.logger_.info("lock expired")
                lock_time_stamp_now = int(self.redis_client.incr(self.assure_lock_key))
                if lock_time_stamp is None and lock_time_stamp_now == 1 \
                        or lock_time_stamp_now == int(lock_time_stamp) + 1:
                    self.logger_.info("delete lock key")
                    self.redis_client.delete(self.lock_key)
                continue
            else:
                time.sleep(self.lock_gap)
                lock_try_times += 1
                self.logger_.debug("retry")
        return flag

    def try_lock(self, seconds):
        assert seconds is not None and isinstance(seconds, int) and seconds >= 1
        max_try_times = int(seconds / self.lock_gap)
        return self.__lock(max_try_times)

    def unlock(self):
        if not self.is_locked():
            raise IllegalLockStateException()
        self.__exit_lock()
        if not self.is_locked():
            return self.redis_client.delete(self.lock_key)
        return True

# main 中是lock 的测试
if __name__ == "__main__":
    from multiprocessing import Process, RawValue
    import os

    redis_client = redis.StrictRedis("localhost", 6379, 0)
    redis_lock = RedisLock(_lock_key="test_lock", _redis_client=redis_client)
    x = RawValue("i", 0)

    def test_lock(shared_x):
        n = 0
        while n < 3:
            if redis_lock.try_lock(3):
                redis_lock.lock()
                n += 1
                shared_x.value += 1
                print "{0} {1} lock success and do {2}".format(os.getpid(),
                                                               threading.currentThread().name, shared_x.value)
                # print "do lock thing"
                time.sleep(0.1)
                redis_lock.unlock()
                redis_lock.unlock()
                # print "release lock and do dump " + str(shared_x.value)
                # print "dump " + str(n) + " done"

    def test_lock_inv(shared_x):
        t_s = []
        for idx in range(5):
            t = threading.Thread(target=test_lock,
                                 args=(shared_x,),
                                 name="[{0}({1})]".format(os.getpid(), idx))
            t.setDaemon(False)
            t.start()
            t_s.append(t)

        for st in t_s:
            st.join()

    def test_no_lock(shared_x):
        n = 0
        while n < 2:
            n += 1
            shared_x.value += 1
            print "{0} lock success and do {1}".format(os.getpid(), shared_x.value)
            # print "do lock thing"
            time.sleep(0.1)
            # print "release lock and do dump " + str(shared_x.value)
            # print "dump " + str(n) + " done"

    for i in range(10):
        Process(target=test_lock_inv, args=(x,)).start()




