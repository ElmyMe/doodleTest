from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from datetime import datetime

def run(brokers = 'localhost:9092', timeout = 20000, topicSrc = 'doodle', topicTrg = 'results'):

    # Calculate unique values for given minute
    def getRes(uids, minutes):
        cnt = len(set(uids))
        tm = datetime.utcfromtimestamp(minutes * 60).strftime('%Y-%m-%d %H:%M:%S')
        print(f"{cnt} unique uids per {tm}")
        return {'minute' : tm, 'count' : cnt}

    # Start reading topic
    consumer = KafkaConsumer(
        'doodleConsumer'
        , bootstrap_servers = brokers
        #, auto_offset_reset='earliest'
        , enable_auto_commit = True
        , consumer_timeout_ms = timeout
        , value_deserializer = lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topicSrc)

    # Process incoming messages
    mn = -1 #minute to aggregate
    uids = []
    res = []
    i = 0 #number of frames
    startTime = None
    for message in consumer:
        if i == 0:
            startTime = datetime.now()
        i += 1
        message = message.value
        # print(message)
        keys = message.keys()
        if 'ts' in keys and 'uid' in keys:
            uts = message['ts']
            nextmn = uts // 60
            if mn == -1:
                mn = nextmn
            # When get minute different from previous one it's time to calculate ids and reset uids list
            # Print the result immediately
            if mn > -1 and nextmn != mn:
                res.append(getRes(uids, mn))
                uids = []
                mn = nextmn
            uids.append(message['uid'])

    finishTime = datetime.now()

    # Calculate and print last portion of uids
    if uids:
        res.append(getRes(uids, mn))

    # Count number of frames per second
    if startTime:
        delta = (finishTime - startTime).total_seconds()
        print(f'Reading: {round(i/delta)} frames per second')

    # Send results to kafka
    if res:
        producer = KafkaProducer(bootstrap_servers=brokers)
        for r in res:
            #print(r)
            producer.send(topicTrg, dumps(r).encode('utf-8'))
    print(f"Result has been sent to {topicTrg}")


if __name__ == '__main__':
    run(timeout = 5000)