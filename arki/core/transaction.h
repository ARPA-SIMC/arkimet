#ifndef ARKI_TRANSACTION_H
#define ARKI_TRANSACTION_H

namespace arki {
namespace core {

/**
 * RAII-style transaction.
 *
 * To work it needs to be embedded inside a Pending container.
 */
class Transaction
{
public:
    virtual ~Transaction();
    virtual void commit() = 0;
    virtual void rollback() = 0;
    virtual void rollback_nothrow() noexcept = 0;
};

/**
 * Represent a pending operation.
 *
 * The operation will be performed when the commit method is called.
 * Otherwise, if either rollback is called or the object is destroyed, the
 * operation will be undone.
 *
 * Copy and assignment have unique_ptr-like semantics
 */
struct Pending
{
    Transaction* trans = nullptr;

    Pending() {}
    Pending(const Pending& p) = delete;
    Pending(Pending&& p);
    Pending(Transaction* trans);
    ~Pending();

    Pending& operator=(const Pending& p) = delete;
    Pending& operator=(Pending&& p);

    /// true if there is an operation to commit, else false
    bool pending() const { return trans != 0; }

    /// Commit the pending operation
    void commit();

    /// Rollback the pending operation
    void rollback();

    /**
     * Rollback the pending operation
     *
     * In case of errors, use nag::warning instead of throwing an exception
     */
    void rollback_nothrow() noexcept;
};

}
}

#endif
