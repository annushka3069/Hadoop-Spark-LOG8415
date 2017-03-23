from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

day=mth=yr=0
vipmembers=canadwomen=maxprize=0
hourm=secm=minm=quartm=0
tcn = ""
def basic_df_example(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    
    df = spark.read.load('data_dump.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
#////////////////////////////////////////HOW MANY MEMBERS \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    nomb_memb = df.groupBy('member_id').agg({'member_id': 'count'}).rdd.map(lambda x: x[0]).count()
    #print("There are "+str(nomb_memb)+" distinct members.")
#//////////////////////OTHER WAY TO DO IT
    #members = df.select("member_id").agg(countDistinct(df['member_id']).alias('c')).collect()
    #for name in members:
    #    print("There are "+str(name.c)+" distinct members.")


#////////////////////////////////////////FIRST DATE OF PURCHASE \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\    
    dates = df.groupBy('date').agg({'date': 'count'})
    v = dates.select(year(dates['date']).alias('year'))
    k = v.agg(min(v['year']).alias('c')).collect()
    for lik in k:
        #print(lik.c)
        yr=lik.c
        p = df.filter(year(df['date']) == lik.c)
        j = p.select(month(p['date']).alias('months'))
        mo = j.agg(min(j['months']).alias('ni')).collect()
        for li in mo:
            #print(li.ni)
            mth=li.ni
            n = p.filter(month(df['date']) == li.ni)
            b = n.select(dayofmonth(n['date']).alias('days'))
            zo = b.agg(min(b['days']).alias('zi')).collect()
            for ti in zo:
                #print(ti.zi)
                 day=ti.zi
                 hr = n.filter(dayofmonth(df['date']) == day)
                 hor = hr.select(hour(hr['date']).alias('hours'))
                 heur = hor.agg(min(hor['hours']).alias('hre')).collect()
                 for h in heur:
                     hourm= h.hre
                     mn = hr.filter(hour(df['date']) == hourm)
                     mnt = mn.select(minute(mn['date']).alias('minutes'))
                     mint = mnt.agg(min(mnt['minutes']).alias('mne')).collect()
                     for m in mint:
                         minm= m.mne
                         sd = mn.filter(minute(df['date']) == minm)
                         seds = sd.select(second(sd['date']).alias('secs'))
                         secnd = seds.agg(min(seds['secs']).alias('sce')).collect()
                         for s in secnd:
                             secm= s.sce
                             qt = sd.filter(second(df['date']) == secm)
                             qrte = qt.select(quarter(qt['date']).alias('quarts'))
                             quarte = qrte.agg(min(qrte['quarts']).alias('qts')).collect()
                             for q in quarte:
                                 quartm = q.qts
   

#////////////////////////////////////////DISTINCTS CREDITS CARDS TYPES \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    creditcards = df.groupBy('card_type').agg({'card_type': 'count'}).rdd.map(lambda x: x[0]).count()

#////////////////////////Other way to do it    

    #creditcards = df.agg(countDistinct(df['card_type']).alias('c')).collect()
    #for types in creditcards:
    #    print("Il ya "+ str(types.c)+" types de cartes de credits dans le dataset")

#////////////////////////////////////////HOW MANY VIP MEMBERS \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    membvip = df.select(df['member_id'], df['vip']).dropDuplicates().filter(col("vip").like("true"))
    nomb_vip = membvip.agg(count(membvip['member_id']).alias('c')).collect() #vip
    for vi in nomb_vip:
       #print("Il ya "+ str(vi.c)+" membres VIP dans le dataset")
       vipmembers = vi.c

#////////////////////////

    #membvip = df.groupBy('vip').agg({'vip': 'count'}).filter(col("vip").like("true")).agg({'vip': 'count'}).rdd.map(lambda x: x[0]).count()
    #nomb_vip = membvip.agg(count(membvip['member_id'])).count()
    #for vi in membvip:
    #   print("Il ya "+ str(vi.c)+" membres VIP dans le dataset")
    #print("Il ya "+ str(membvip)+" membres VIP dans le dataset")

#////////////////////////////////////////CANADIEN WOMANS ZONE 7\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\  
      
    femca = df.select(df['member_id'], df['gender'], df['country'], df['zone']).dropDuplicates().filter(col("gender").like("Female")).filter(col("country").like("CA")).filter(col("zone").like("zone7"))
    somefem = femca.dropDuplicates()
    #vip = somevip.filter(col("vip").like("true"))
    nomb_fem = femca.agg(count(femca['member_id']).alias('c')).collect()
    for fi in nomb_fem:
        #print("Il ya "+ str(fi.c)+" femmes canadiennes qui ont achete quelque chose dans la zone 7")
        canadwomen = fi.c

#////////////////////////////////////////MAX PRODUCT PRIZE \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    prodpri = df.select(df.amount.alias('s'))
    prod = prodpri.agg(max(prodpri['s']).alias('c')).collect()
    for lik in prod:
         #print(lik.c)
         maxprize = lik.c

#/////////////////////////////////////////////////////////////////////////
    #somefem = femca.dropDuplicates()
    #vip = somevip.filter(col("vip").like("true"))
    #nomb_fem = femca.agg(count(femca['member_id']).alias('c')).collect()
    #for fi in nomb_fem:
    #   print("Il ya "+ str(fi.c)+" femmes canadiennes qui ont achete quelque chose dans la zone 7")
    #print("Il ya "+str(nomb_vip)+" membres VIP dans le dataset")
    #nomb_vip = vip.agg(count(vip['vip']).alias('c')).collect()
    #for vi in nomb_vip:
    #    print("Il ya "+ str(vi.c)+" membres VIP dans le dataset")
#////////////////////////////////////////MAX PURCHASE PER COUNTRY \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\   
    topcn = df.groupBy('country').agg({'product_id' : 'count'})
    tocn = topcn.agg(max(topcn['country']).alias('v')).collect()
    for b in tocn:
       tcn = b.v
    #tocn = topcn.rdd.map(lambda d:d[1]).reduce(lambda x, y: x + y)
    #topcn.select("country").rdd.flatMap(lambda x: x).histogram(1)
    #topcn.orderBy(desc("count(product_id)")).collect()
    #topcn = df.select("country", "product_id").groupBy("country").show()
    #tc = topcn.groupBy(topcn['c']).show
    #for name in topcn:
        #print(name)
    #cn = topcn.rdd.map(lambda d: d[1]).reduce(lambda a, b: a + b)
    #print(cn)
    #tcn = topcn.agg(max(topcn['country'])).collect()
    #for b in tcn:
     #  print(b)




#////////////////////////////////////////DISPLAYING RESULTS \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

    print("There are "+str(nomb_memb)+" distinct members.")
    print ("The first purchase happened on "+str(day)+"/"+str(mth)+"/"+str(yr)+", more precisely at "+str(hourm)+":"+str(minm)+":"+str(secm)+":"+str(quartm)+" .")
    print("There are "+ str(creditcards)+" different credit card types in the dataset .")
    print("There are "+ str(vipmembers)+" VIP members in the dataset .")
    print("There are "+ str(canadwomen)+" canadian women who purchased something on zone 7 .")
    print("The maximum prize for a product is "+ str(maxprize)+" .")
    print("The country with the biggest amount of purchased products is "+ tcn+" .")
#////////////////////////////////////////MAIN CLASS \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Experiments on Python Spark SQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    basic_df_example(spark)
    spark.stop()
