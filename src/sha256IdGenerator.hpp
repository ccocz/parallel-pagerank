#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include <fstream>
#include <array>
#include <mutex>
#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        FILE *output;
        std::string cmd("echo -n \"" + content + "\" | " + "sha256sum");
        output = popen(&cmd[0], "r");
        if (output == nullptr) {
            std::cerr << "popen failed";
            exit(1);
        }
        std::array<char, 128> buffer{};
        if (fgets(buffer.data(), 128, output) == nullptr) {
            std::cerr << "fgets failed";
            exit(1);
        }
        if (pclose(output) == -1) {
            std::cerr << "pclose failed";
            exit(1);
        }
        return std::string(&buffer[0], 64);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
