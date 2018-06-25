#include "MapReduceFramework.h"
#include "Barrier.h"
#include <iostream>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <semaphore.h>
#include <pthread.h>

using std::vector;
using std::sort;
using std::cerr;
using std::string;
using std::all_of;
typedef std::vector<IntermediatePair> ShuffleVec;
typedef std::vector<ShuffleVec> ReduceVec;

// ---------- context class ---------- //

// represents each thread context
class ThreadContext{
    private:
        int threadID;
        std::atomic<int>* atomic_ref;// used to synchronize the threads when reading from the input vector
        const MapReduceClient* theClient;
        pthread_mutex_t* theMutex;

    public:
        ReduceVec* beforeReduceVec;
        vector<IntermediatePair> outputValFromMap;
        const InputVec* theInput;
        OutputVec* theOutput;
        Barrier* barrier;
        // vector common for all the threads, which its content is a platform for the shuffle result
        vector<IntermediateVec>* globalIntermediatePairs;
        sem_t* mySemaphore;
        int* inShuffle;
        pthread_mutex_t* reduceMutex;
        pthread_mutex_t* shuffleMutex;

        ThreadContext() = default;
        ThreadContext(int threadID, std::atomic<int>* ref, Barrier* bar,
                      const MapReduceClient* client, const InputVec* input,
                      OutputVec* output, pthread_mutex_t* mutex, ReduceVec* redVec,
                      sem_t* sem, int* inShuf, pthread_mutex_t* redMutex,
                      vector<IntermediateVec>* globalInterPairs, pthread_mutex_t* shufMutex) :
                        threadID(threadID), atomic_ref(ref), theClient(client),
                        theMutex(mutex), beforeReduceVec(redVec), theInput(input),
                        theOutput(output), barrier(bar),
                        globalIntermediatePairs(globalInterPairs), mySemaphore(sem),
                        inShuffle(inShuf), reduceMutex(redMutex), shuffleMutex(shufMutex) {}

        int getThreadID() const
        {
            return threadID;
        }
        std::atomic<int> *getAtomic_ref() const
        {
            return atomic_ref;
        }
        const MapReduceClient *getTheClient() const
        {
            return theClient;
        }
        const InputVec *getTheInput() const
        {
            return theInput;
        }
        pthread_mutex_t *getTheMutex() const
        {
            return theMutex;
        }
};

// ---------- ------------- ---------- //

// this is the comparator for the sorting
struct sortComparator {
    bool operator()(const IntermediatePair &left, const IntermediatePair &right) {
        return *left.first<*right.first; // we need the sorted values to be in ascending order
    }
};

// this is the function that each thread runs
void* start(void* arg){
    // read input and map
    auto* myContext = (ThreadContext*) arg;
    int old_value = (*(myContext->getAtomic_ref()))++;
    while ((size_t)old_value < myContext->getTheInput()->size()){
        myContext->getTheClient()->map(myContext->theInput->at(old_value).first,
                                       myContext->theInput->at(old_value).second,
                                       &myContext->outputValFromMap);
        old_value = (*(myContext->getAtomic_ref()))++;
    }
    // sort
    sort(myContext->outputValFromMap.begin(), myContext->outputValFromMap.end(), sortComparator());
    // push into the global vector the thread's sorted vector
    pthread_mutex_lock(myContext->getTheMutex());
    myContext->globalIntermediatePairs->push_back(myContext->outputValFromMap);
    pthread_mutex_unlock(myContext->getTheMutex());
    // barrier
    myContext->barrier->barrier();
    // shuffle
    if (myContext->getThreadID() == 0){
        while (true){
            K2* K2max = nullptr;
            size_t globalVectorCtr = myContext->globalIntermediatePairs->size();
            // finding the maximum key (from K2 type)
            for (auto &vecInterPairs : *myContext->globalIntermediatePairs) {
                if (vecInterPairs.empty()) {
                    globalVectorCtr--;
                    continue;
                }
                K2* tmpMax = vecInterPairs.back().first;
                if (K2max == nullptr){
                    K2max = tmpMax;
                } else {
                    if (*K2max < *tmpMax){
                        K2max = tmpMax;
                    }
                }
            }
            if(globalVectorCtr == 0)
                break;
            ShuffleVec tmp;
            // create the vector of pairs that their key is K2max
            for (auto &vecInterPairs : *myContext->globalIntermediatePairs) {
                while (!vecInterPairs.empty()
                      && !(*vecInterPairs.back().first < *K2max)) {
                    tmp.emplace_back(vecInterPairs.back().first, vecInterPairs.back().second);
                    vecInterPairs.erase(vecInterPairs.end());
                }
            }
            pthread_mutex_lock(myContext->getTheMutex());
            myContext->beforeReduceVec->push_back(tmp);
            tmp.clear();
            pthread_mutex_unlock(myContext->getTheMutex());
        }
        pthread_mutex_lock(myContext->shuffleMutex);
        *(myContext->inShuffle) = 0;
        pthread_mutex_unlock(myContext->shuffleMutex);
    }
    // reduce
    pthread_mutex_lock(myContext->shuffleMutex);
    int shuf = *(myContext->inShuffle); // in-shuffle condition
    pthread_mutex_unlock(myContext->shuffleMutex);

    pthread_mutex_lock(myContext->getTheMutex());
    bool empty = myContext->beforeReduceVec->empty(); // empty vector condition
    pthread_mutex_unlock(myContext->getTheMutex());

    while ((!empty) || (shuf == 1)){
        sem_wait(myContext->mySemaphore);
        pthread_mutex_lock(myContext->getTheMutex());
        if (!myContext->beforeReduceVec->empty()){
            myContext->getTheClient()->reduce(&myContext->beforeReduceVec->back(), myContext);
            myContext->beforeReduceVec->pop_back();
        }
        pthread_mutex_unlock(myContext->getTheMutex());

        pthread_mutex_lock(myContext->shuffleMutex);
        shuf = *(myContext->inShuffle); // in-shuffle condition
        pthread_mutex_unlock(myContext->shuffleMutex);

        pthread_mutex_lock(myContext->getTheMutex());
        empty = myContext->beforeReduceVec->empty(); // empty vector condition
        pthread_mutex_unlock(myContext->getTheMutex());

        sem_post(myContext->mySemaphore);
    }
    return arg;
}

// this is the lib function supporting client map
void emit2 (K2* key, V2* value, void* context){
    auto* output = (vector<IntermediatePair>*)context;
    output->push_back(IntermediatePair(key, value));
}

// this is the lib function supporting client reduce
void emit3 (K3* key, V3* value, void* context){ // creates the output vector
    auto* cont = (ThreadContext*) context;
    pthread_mutex_lock(cont->reduceMutex);
    cont->theOutput->push_back({key, value});
    pthread_mutex_unlock(cont->reduceMutex);
}

// this is the main function - operates the lib running
void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t shuffleMutex = PTHREAD_MUTEX_INITIALIZER;
    std::atomic<int> atomic_counter(0);
    auto* threads = new pthread_t[multiThreadLevel]; // threads pool
    auto* contexts = new ThreadContext[multiThreadLevel];
    ReduceVec beforeReduceVec = {};
    sem_t mySemaphore{};
    sem_init(&mySemaphore, 0, 1);
    Barrier barrier(multiThreadLevel);
    int inShuffle = 1;
    vector<IntermediateVec> globalIntermediatePairs(0);
    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = ThreadContext{i, &atomic_counter, &barrier, &client,
                                    &inputVec, &outputVec, &mutex, &beforeReduceVec,
                                    &mySemaphore, &inShuffle, &reduceMutex, &globalIntermediatePairs,
                                    &shuffleMutex};
    }
    for (int i = 0; i < multiThreadLevel; ++i){
        threads[i] = 0; // for valgrind error
    }
    for (int i = 1; i < multiThreadLevel; ++i) {
        if (pthread_create(threads + i, nullptr, start, contexts + i) != 0){
            cerr << "pthread-create error\n";
            delete[] threads;
            delete[] contexts;
            exit(EXIT_FAILURE);
        }
    }
    start(&contexts[0]);
    for (int i = 1; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], nullptr);
    }
    if (pthread_mutex_destroy(&mutex) != 0) {
        cerr << "mutex destroy failed1\n";
        delete[] threads;
        delete[] contexts;
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_destroy(&reduceMutex) != 0) {
        cerr << "mutex destroy failed2\n";
        delete[] threads;
        delete[] contexts;
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_destroy(&shuffleMutex) != 0) {
        cerr << "mutex destroy failed3\n";
        delete[] threads;
        delete[] contexts;
        exit(EXIT_FAILURE);
    }
    if (sem_destroy(&mySemaphore) != 0) {
        cerr << "semaphore destroy failed\n";
        delete[] threads;
        delete[] contexts;
        exit(EXIT_FAILURE);
    }
    delete[] threads;
    delete[] contexts;
}