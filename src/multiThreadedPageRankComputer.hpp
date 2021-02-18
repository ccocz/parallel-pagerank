#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <condition_variable>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg){};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {

        std::vector<std::thread> threads;

        for (uint32_t i = 0, start = 0; i < numThreads && start < network.getSize(); i++) {
            threads.push_back(std::thread{[start, &network, this]{
                  for (uint32_t i = start;
                       i < network.getSize() && i <= start + network.getSize() / numThreads;
                       i++) {
                      network.getPages()[i].generateId(network.getGenerator());
                  }
                }});
            start += network.getSize() / numThreads + 1;
        }

        for (auto &thread : threads) {
            thread.join();
        }

        threads.clear();

        init(network);

        std::vector<PageId> division[numThreads];
        uint32_t last = 0;

        for (auto& page : network.getPages()) {
            last += division[last].size() == network.getSize() / numThreads + 1;
            division[last].push_back(page.getId());
        }

        double difference;
        currentAlpha = alpha;

        for (uint32_t i = 0; i < iterations; ++i) {

            difference = 0;
            dangleSum = newDangleSum;
            newDangleSum = 0;
            dangleSum = dangleSum * alpha;
            result.clear();
            readyThreads = 0;

            for (uint32_t j = 0; j < numThreads; j++) {
                threads.push_back(std::thread(&MultiThreadedPageRankComputer::workerThread,
                                              this,
                                              std::ref(division[j]),
                                              std::ref(network),
                                              std::ref(difference)));
            }
            for (auto& thread : threads) {
                thread.join();
            }

            threads.clear();

            ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

            if (difference < tolerance) {
                return result;
            }
        }
        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;

    mutable std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
    mutable std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
    mutable std::unordered_set<PageId, PageIdHash> danglingNodes;
    mutable std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
    mutable std::vector<PageIdAndRank> result;
    mutable double dangleSum;
    mutable double currentAlpha;
    mutable double newDangleSum;
    mutable std::mutex mutex;
    mutable std::condition_variable barrier;
    mutable uint32_t readyThreads;

    void workerThread(const std::vector<PageId>& myDivision,
                      Network const& network,
                      double& difference) const {

        std::vector<std::pair<PageId, double>> results;
        double differenceLocal = 0;
        double danglingSumLocal = 0;

        for (PageId pageId : myDivision) {
            double danglingWeight = 1.0 / network.getSize();
            double myPR = dangleSum * danglingWeight + (1.0 - currentAlpha) / network.getSize();
            if (edges.count(pageId) > 0) {
                for (auto link : edges[pageId]) {
                    myPR += currentAlpha * pageHashMap[link] / numLinks[link];
                }
                differenceLocal += std::abs(pageHashMap[pageId] - myPR);
            }
            if (danglingNodes.count(pageId)) {
                danglingSumLocal += myPR;
            }
            results.push_back({pageId, myPR});
        }

        std::unique_lock<std::mutex> uq(mutex);

        readyThreads++;
        for (auto ans : results) {
            result.push_back(PageIdAndRank(ans.first, ans.second));
        }
        difference += differenceLocal;
        newDangleSum += danglingSumLocal;

        if (readyThreads == numThreads) {
            uq.unlock();
            barrier.notify_all();
        } else {
            barrier.wait(uq, [this]{return readyThreads >= numThreads;});
            uq.unlock();
        }

        for (auto ans : results) {
            pageHashMap[ans.first] = ans.second;
        }

    }

    void init(Network const& network) const {

        clear();

        for (auto const& page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        newDangleSum = 0;

        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
                newDangleSum += 1.0 / network.getSize();
            }
        }

        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }
    }

    void clear() const {
        pageHashMap.clear();
        numLinks.clear();
        danglingNodes.clear();
        edges.clear();
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
