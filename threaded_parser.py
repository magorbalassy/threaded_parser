import logging, os, sys

from multiprocessing import cpu_count
from threading import Thread
from Queue import Queue

global convertFinished
convertFinished = False

def getpstlist(folder):
    filelist = []
    for root, dirs, files in os.walk(folder):
        for filename in files:
            if filename[-4:].lower() == '.pst' :
                filelist.append(str(root + '/' + filename))
    return filelist

# Threaded function for queue processing.
def pst_jsonl(pst_q, result, thread_nr):
    while not pst_q.empty():
        work = pst_q.get()                    #fetch new work from the Queue
        result[work[0]] = work[1]             #convert_pst_jsonl(work[1])
        logging.info("Thread %d - conversion result : %s" %(thread_nr,result[work[0]]))
        #signal to the queue that task has been processed
        pst_q.task_done()
    return True

if __name__ == "__main__":
    logging.basicConfig(filename='parser.log',level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('==== Starting new run ====')
    if len(sys.argv) < 2 :
        logging.error('Missing argument.')
        sys.exit(1)
    elif not os.path.isdir(sys.argv[1]) :
        logging.error('Argument must be a folder.')
        sys.exit(1)
    # making sure that we use absolute path
    pst_folder = os.path.abspath(sys.argv[1])
    filelist = getpstlist(pst_folder)
    filelist.sort()
    if len(filelist) == 0 :
        logging.info('No files found in folder %s.' % sys.argv[1])
        sys.exit(0)
    else:
        logging.info('Found %d files in folder %s :' % (len(filelist),sys.argv[1]))
    #Populating Queues with tasks and initializing threads
    pst_q = Queue(maxsize=0)
    jsonl_q = Queue(maxsize=0) #not yet used
    threads = []    
    for i,f in enumerate(filelist) : 
        logging.info('Queueing %d, %s' %(i,f))
        pst_q.put((i,filelist[i]))
        jsonl_q.put((i,filelist[i]))
    num_threads = cpu_count()-28
    logging.info('Number of CPUs : %d, using %d threads.' % (cpu_count(),num_threads))
    results = ['' for x in filelist]
    #while not pst_q.empty():
    #    print pst_q.get()
    for i in range(num_threads):
        logging.info('Starting thread %d ' % i)
        worker = Thread(target=pst_jsonl, args=(pst_q,results,i))
        worker.setDaemon(True)    #setting threads as "daemon" allows main program to 
        worker.start()            #exit eventually even if these dont finish correctly.
    #now we wait until the queue has been processed
    logging.info('Threads started, waiting to complete.')
    pst_q.join()
    convertFinished = True
    logging.info('All tasks completed. %s' %convertFinished)
