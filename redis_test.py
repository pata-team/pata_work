# -*- coding: UTF-8 -*-
__author__ = 'wangkaixian'
import redis
from multiprocessing import Process,Lock
def func_test(i,l):
    print 'this is proc: ',i
    # n = 0
    # while n<5:
    r = redis.Redis(host='localhost',port=6379,db=0)
    try:
        # l.acquire()
        # id = r.get("id")
        print 'proc %s id is : %s'%(str(i),r.incr('id'))
        # r.incr("id")
        # id = int(id) + 1
        # r.set('id',id)
        # l.release()
    except:
        pass
        # r.set('id',1)
        # l.release()
        # n = n + 1
if __name__=='__main__':
    l=Lock()
    for i in range(5):
        p = Process(target=func_test,args=(i,l))
        # p.start()
        # p.join()
    r = redis.Redis(host='localhost',port=6379,db=0)
    # print r.get(2)
    # print r.incr('id')
    # print r.incr('is')
    # r.hset('car','name','bmw')
    # r.hset('car','price','100W')
    # print r.hvals('car')
    # print r.hlen('car')
    # print r.llen('car')
    # print r.hgetall('car')['price']

    value = [str(r"sid"),str("cid"), str("sig_sid"), str("timestamp"),str("ip_src"), str("sport"),str("ip_dst"), str("dport"),"url",str("rep_times"),'0']
    #插入操作
    insert_id = r.incr('id')
    #将每条事件的id插入专门的id列表
    r.lpush('insert_id',insert_id)
    #将每条id所对应的事件写入散列之中
    r.hmset(insert_id,mapping={'sid':value[0],'cid':value[1],'sig_sid':value[2]})
    #id列表专门用来统计事件个数
    numberofEvent=r.llen('insert_id')
    #当事件个数大于一定的值，开始删除（发布表的维护）
    if numberofEvent > 10:
        remove_id=r.rpop('insert_id')
        print 'remove_id: ',remove_id
        #删除对应的事件内容
        r.hdel(remove_id,'sid','cid','sig_sid')
    #更新操作
    insert_id_list = r.lrange('insert_id',0,10)
    #通过归并向量得到需要归并的字段
    sid='sid'
    cid='cid'
    is_update=['sid','cid']
    print r.hmget('17',is_update)
    #对id列表中所有id进行比对
    for update_id in insert_id_list:
        #如果有一个id对应的事件有所匹配，该id为需要更新的id
        if r.hmget(update_id,is_update) == [sid,cid]:
            print 'update_id: ',update_id
            #将对应id事件的时间与重复次数更新
            timestamp=value[3]
            # rep_times=str(int(r.hget(update_id,'rep_times'))+1)
            r.hmset(update_id,mapping={'sid':value[0]+'s','cid':value[1]+'d'})

    #历史库更新
    update_id
    timestamp
    # rep_times
    #将上面三项插入update_point

    #历史库搬家
    #start_point与current_point
    #start_point为从历史表中的最大id，curret_id为当前insert_id
    start_id = 1
    # current_id = r.lrange('insert_id',0,10)[0]
    current_id = r.get('id')
    print 'current_id: ',current_id
    if start_id < current_id:
        for move_id in range(int(start_id)+1,int(current_id)+1):
            move_event = r.hgetall(move_id)
            print move_event
            #mysql插入操作
        start_id = current_id
    else:
        current_id = r.get('id')
    # 事件队列写入操作
    tableNum = str(1)
    r.hmset(tableNum,mapping={'id':'1','sid':'01','cid':'001'})
    # 从事件队列中读取数据
    sid1 = r.hgetall(tableNum)['sid']
    id1 = r.hgetall(tableNum)['id']
    cid1 = r.hgetall(tableNum)['cid']
    print 'id is %s, sid is %s, cid is %s.'%(id1,sid1,cid1)


    print r.hgetall('4')
    # print r.hmget('1589',sid,cid)
    # print r.hget('1598',sid)
# k = int(r.get(1)) + 1
# r.set(2,k)
# print r.get(1)
# print r.keys()
# print r.incr(k)
#r.expire('wang',20)
#r.set('wang','kx')