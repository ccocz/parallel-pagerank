#ifndef SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class SingleThreadedPageRankComputer : public PageRankComputer {
public:
    SingleThreadedPageRankComputer() {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        for (auto const& page : network.getPages()) {
            page.generateId(network.getGenerator());
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        double newDangleSum = 0;

        std::unordered_set<PageId, PageIdHash> danglingNodes;
        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
                newDangleSum += 1.0 / network.getSize();
            }
        }

        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        std::vector<PageId> ids;

        for (auto& page : network.getPages()) {
            ids.push_back(page.getId());
        }

        std::vector<PageIdAndRank> result;

        double difference;
        double dangleSum;

        for (uint32_t i = 0; i < iterations; ++i) {

            dangleSum = newDangleSum;
            dangleSum = dangleSum * alpha;
            newDangleSum = 0;
            result.clear();

            difference = 0;
            std::vector<std::pair<PageId, double>> results;

            for (PageId pageId : ids) {

                double danglingWeight = 1.0 / network.getSize();
                double myPR = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        myPR += alpha * pageHashMap[link] / numLinks[link];
                    }
                    difference += std::abs(pageHashMap[pageId] - myPR);
                }
                if (danglingNodes.count(pageId)) {
                    newDangleSum += myPR;
                }

                results.push_back({ pageId, myPR });
            }

            for (auto iter : results) {
                result.push_back(PageIdAndRank(iter.first, iter.second));
                pageHashMap[iter.first] = iter.second;
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
        return "SingleThreadedPageRankComputer";
    }
};

#endif /* SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_ */
