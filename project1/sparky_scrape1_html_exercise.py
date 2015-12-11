from bs4 import BeautifulSoup as bs
import os, sys, logging, string, glob
import json
import re

#Import relevant Spark libraries
from pyspark ...

def clean_text(text_as_list):
    text_as_string = " ".join(text_as_list)
    text_as_string = text_as_string.encode("utf8").translate(None,'=@&$/%?<>,[]{}()*.0123456789:;-\n\'"_').lower()
    text_as_string = re.sub(' +',' ',text_as_string)

    return text_as_string


#Modify the parse_page method. Are any modifications necessary
def parse_page(input_page_as_tuple):

    ....

    return doc

def parse_text(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - textdata: a list of parsed text output by looping over html paragraph tags
        note:
            - could soup.get_text() instead but the output is more noisy """
    textdata = ['']

    for tag in soup.find_all("div", {"class":"text"}):
        try:
           textdata.append(tag.text.encode('ascii','ignore').strip())
        except Exception:
           continue

    for text in soup.find_all('p'):
        try:
            textdata.append(text.text.encode('ascii','ignore').strip())
        except Exception:
            continue

    textdata = filter(None,textdata)
    return clean_text(textdata)


def parse_title(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - title: parsed title """

    title = ['']

    try:
        title.append(soup.title.string.encode('ascii','ignore').strip())
    except Exception:
        return title

    return filter(None,title)

def parse_links(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - linkdata: a list of parsed links by looping over html link tags
        note:
            - some bad links in here, this could use more processing """

    linkdata = ['']

    for link in soup.find_all('a'):
        try:
            linkdata.append(str(link.get('href').encode('ascii','ignore')))
        except Exception:
            continue

    return filter(None,linkdata)


def parse_images(soup):
    """ parameters:
            - soup: beautifulSoup4 parsed html page
        out:
            - imagesdata: a list of parsed image names by looping over html img tags """
    imagesdata = ['']

    for image in soup.findAll("img"):
        try:
            imagesdata.append("%(src)s"%image)
        except Exception:
            continue

    return filter(None,imagesdata)

def main(argv,npartitions):
    #Initialize Spark context
    sc = ...

    #Read whole files in the following folder: /user/alexeys/BigDataCourse/web_dataset/ into a pair RDD of type
    #document unique ID - document contents as string tuple
    #Apply the parse_page method to parse each page (first map() transformation)
    #Convert everything to JSON format (second map() transformation)
    fIn_rdd = sc.wholeTextFiles("/scratch/network/alexeys/BigDataCourse/web_dataset/2/",npartitions).map() ... map()

    #do not repartition - we are running with 1 partition for testing purposes
    fIn_rdd.saveAsTextFile(os.environ.get('SCRATCH_PATH')+'/BigDataCourse/web_dataset_preprocessed2/')

if __name__ == "__main__":
   import time
   start = time.time()
   npartitions = 1
   main(sys.argv,npartitions)
   end = time.time()
   print "Elapsed time: ", end-start
