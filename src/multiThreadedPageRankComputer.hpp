#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

//todo : mutable

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;

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

        for (auto const& page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        std::unordered_set<PageId, PageIdHash> danglingNodes;
        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
            }
        }

        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        std::vector<PageId> division[numThreads];
        uint32_t last = 0;

        for (auto& pageMapElem : pageHashMap) {
            last += division[last].size() == network.getSize() / numThreads + 1;
            division[last].push_back(pageMapElem.first);
        }

        IterationHelper helper(alpha, network, edges, numLinks, numThreads, division, danglingNodes);

        double difference;

        for (uint32_t i = 0; i < iterations; ++i) {
            std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap = pageHashMap;
            difference = helper.newIteration(previousPageHashMap, pageHashMap);

            std::vector<PageIdAndRank> result;
            for (auto iter : pageHashMap) {
                result.push_back(PageIdAndRank(iter.first, iter.second));
            }

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

    class IterationHelper {

    public:
        IterationHelper(double alpha,
                        Network const& network,
                        std::unordered_map<PageId, std::vector<PageId>, PageIdHash>& edges,
                        std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks,
                        uint32_t numThreads,
                        std::vector<PageId>* division,
                        std::unordered_set<PageId, PageIdHash> danglingNodes)
            : numThreads(numThreads)
            , alpha(alpha)
            , network(network)
            , edges(edges)
            , numLinks(numLinks)
            , division(division)
            , danglingNodes(danglingNodes) {};

        double newIteration(std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
            std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap)
        {
            dangleSum = 0;
            for (auto danglingNode : danglingNodes) {
                dangleSum += previousPageHashMap[danglingNode];
            }
            dangleSum = dangleSum * alpha;

            for (auto& pageMapElem : pageHashMap) {
                PageId pageId = pageMapElem.first;
                double danglingWeight = 1.0 / network.getSize();
                pageMapElem.second = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();
            }

            double difference = 0;

            std::vector<std::thread> threads;

            for (uint32_t i = 0; i < numThreads; i++) {
                threads.push_back(std::thread(&IterationHelper::workerThread,
                    this,
                    std::ref(division[i]),
                    std::ref(previousPageHashMap),
                    std::ref(pageHashMap),
                    std::ref(difference)));
            }
            for (auto& thread : threads) {
                thread.join();
            }
            return difference;
        }

    private:
        uint32_t numThreads;
        double dangleSum;
        double alpha;
        Network const& network;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash>& edges;
        std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks;
        std::vector<PageId>* division;
        std::mutex mutex;
        std::unordered_set<PageId, PageIdHash> danglingNodes;

        void workerThread(const std::vector<PageId>& myDivision,
            std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
            std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
            double& difference)
        {
            for (PageId pageId : myDivision) {
                double myPR = pageHashMap[pageId];
                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        myPR += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                    std::lock_guard<std::mutex> lockGuard(mutex);
                    pageHashMap[pageId] = myPR;
                    difference += std::abs(previousPageHashMap[pageId] - pageHashMap[pageId]);
                }
            }
        }
    };
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
