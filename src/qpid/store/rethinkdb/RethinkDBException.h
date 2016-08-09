#ifndef RETHINKDB_EXCEPTION_H
#define RETHINKDB_EXCEPTION_H

#include <boost/format.hpp>
#include <rethinkdb.h>

namespace qpid {
namespace store {
namespace rethinkdb {

class RethinkDBException : public std::exception
{
    std::string text;
public:
    RethinkDBException(const std::string& _text)
      : text(_text) {}
    RethinkDBException(const std::string& _text, const R::Error& cause)
      : text(_text + ": " + cause.message) {}
    virtual ~RethinkDBException() throw() {}
    virtual const char* what() const throw() { return text.c_str(); }
};

#define THROW_RDB_EXCEPTION(MESSAGE) \
  throw RethinkDBException(boost::str(boost::format("%s (%s:%d)") % (MESSAGE) % __FILE__ % __LINE__))
#define THROW_RDB_EXCEPTION_2(MESSAGE, EXCEPTION) \
  throw RethinkDBException(boost::str(boost::format("%s (%s:%d)") % (MESSAGE) % __FILE__ % __LINE__), EXCEPTION)

}}}

#endif // ifndef RETHINKDB_EXCEPTION_H
