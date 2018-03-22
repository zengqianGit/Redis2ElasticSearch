# _*_ coding: utf-8 _*_
__author__ = 'qianzeng'
__date__ = '2018/3/20 0:40'
import redis
from time import sleep
import es_types
from elasticsearch_dsl.connections import connections

connections.create_connection(hosts=["localhost"])
es = connections.create_connection(es_types.JdSpiderType._doc_type.using)
item_num = 0
except_num = 0

# TODO
# except过多，原因包括item_id、price、name


def gen_suggests(index, info_tuple):
    #根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            #调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"])>1])
            new_words = anylyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})

    return suggests


def do_execute(item_str):
    global except_num
    try:
        item = eval(item_str)

        jd = es_types.JdSpiderType()
        jd.item_id = item["item_id"]
        jd.name = item["name"]
        jd.summary = item["summary"]
        jd.price = float(item["price"])
        jd.tag_1 = item["tag_1"]
        jd.tag_2 = item["tag_2"]
        jd.tag_3 = item["tag_3"]
        jd.tag_4 = item["tag_4"]
        jd.dianpu_name = item["dianpu_name"]
        jd.jself = item["jself"]
        jd.crawl_time = item["crawl_time"]
        jd.suggest = gen_suggests(es_types.JdSpiderType._doc_type.index, ((jd.name, 10), (jd.dianpu_name, 8), (jd.summary, 6)))
        # w = 8
        # try:
        #     x = int(jd.jself)
        #     if x:
        #         w = 10
        # except Exception as e:
        #     print(e)
        # jd.suggest = gen_suggests(es_types.JdSpiderType._doc_type.index, ((jd.name, w), (jd.summary, 6)))

        jd.save()
    except Exception as e:
        except_num += 1
        print(e)
        print(item_str)
        print("-"*10)


def getItem():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    while r.llen('jd_spider:items'):
        global item_num
        if (r.llen('jd_spider:items')-1):
            d = str(r.lpop('jd_spider:items'), encoding = "utf8")
            do_execute(d)
            item_num += 1
        else:
            print("sleep now...item_num:{0}, except_num:{1}".format(item_num, except_num))
            sleep(60)
            print("sleep down...")


getItem()