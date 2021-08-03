from udfTesting import wordCountTest, distributedGrepTest, URLFrequencyTest
import json


### -- Word Count
# Open the Word count JSON file
f = open('metadata/wordcountmetadata.json', 'r')
wordcount_config = f.read()
f.close()
wordcount_config = json.loads(wordcount_config)
wordCountTest.test(wordcount_config["inputfile"], wordcount_config["outputdir"])

### -- URL Frequency
# Open the URL Frequency JSON file
f = open("metadata/urlfrequencymetadata.json", 'r')
urlfrequency_config = f.read()
f.close()
urlfrequency_config = json.loads(urlfrequency_config)
URLFrequencyTest.test(urlfrequency_config["inputfile"], urlfrequency_config["outputdir"])

### -- Distributed Grep
# Open the Distributed Grep JSON file
f = open("metadata/distributedgrepmetadata.json", 'r')
distributedgrepconfig = f.read()
f.close()
distributedgrepconfig = json.loads(distributedgrepconfig)
distributedGrepTest.test(distributedgrepconfig["inputfile"], distributedgrepconfig["outputdir"], "distributed")

