package bank

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Bank represents a simple banking system
type Bank struct {
	bankLock *sync.Mutex
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}

// initializes a new bank
func BankInit() *Bank {
	b := Bank{&sync.Mutex{}, make(map[int]*Account)}
	return &b
}

func (b *Bank) notifyAccountHolder(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) notifySupportTeam(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) logInsufficientBalanceEvent(accountID int) {
	DPrintf("Insufficient balance in account %d for withdrawal\n", accountID)
}

func (b *Bank) sendEmailTo(accountID int) {
	// Dummy function
	// hello
	// marker
	// eraser
}

// creates a new account with the given account ID
func (b *Bank) CreateAccount(accountID int) {
	// your code here
	//lock the bank to prevent concurrent modification of the accounts map
	b.bankLock.Lock()
	defer b.bankLock.Unlock()

	//check if the account ID already exists to avoid duplicates
	if _, exists := b.accounts[accountID]; exists {
		//if already exists, print an error message
		fmt.Println("account ID already exists")
	} else {
		// If the account ID does not exist, create a new account with balance = 0
		b.accounts[accountID] = &Account{
			balance: 0,
			lock:    &sync.Mutex{},
		}
	}

}

// deposit a given amount to the specified account
func (b *Bank) Deposit(accountID, amount int) {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][DEPOSIT] for account %d\n", accountID)

	newBalance := account.balance + amount
	account.balance = newBalance

	DPrintf("Deposited %d into account %d. New balance: %d\n", amount, accountID, newBalance)

	//b.accounts[accountID].lock.Unlock()
	//this directly unlock the mutex associated with the account while the above line
	//release the one retrieved from the accounts map stored in bank which may caused data race
	account.lock.Unlock()
	DPrintf("[RELEASED LOCK][DEPOSIT] for account %d\n", accountID)
}

// withdraw the given amount from the given account id
func (b *Bank) Withdraw(accountID, amount int) bool {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][WITHDRAW] for account %d\n", accountID)

	if account.balance >= amount {
		newBalance := account.balance - amount
		account.balance = newBalance
		DPrintf("Withdrawn %d from account %d. New balance: %d\n", amount, accountID, newBalance)
		account.lock.Unlock()
		DPrintf("[RELEASED LOCK][WITHDRAW] for account %d\n", accountID)
		return true
	} else {
		//release the lock if the if statement is not triggered
		account.lock.Unlock()
		// Insufficient balance in account %d for withdrawal
		// Please contact the account holder or take appropriate action.
		// trigger a notification or alert mechanism
		b.notifyAccountHolder(accountID)
		b.notifySupportTeam(accountID)
		// log the event for further investigation
		b.logInsufficientBalanceEvent(accountID)

		return false
	}
}

// transfer amount from sender to receiver
func (b *Bank) Transfer(sender int, receiver int, amount int, allowOverdraw bool) bool {
	b.bankLock.Lock()
	senderAccount := b.accounts[sender]
	receiverAccount := b.accounts[receiver]
	//b.bankLock.Unlock()

	senderAccount.lock.Lock()
	receiverAccount.lock.Lock()

	// if the sender has enough balance,
	// or if overdraws are allowed
	success := false
	if senderAccount.balance >= amount || allowOverdraw {
		senderAccount.balance -= amount
		receiverAccount.balance += amount
		success = true
	}
	senderAccount.lock.Unlock()
	receiverAccount.lock.Unlock()
	//unlock after completing transfer
	b.bankLock.Unlock()
	return success
}

func (b *Bank) DepositAndCompare(accountId int, amount int, compareThreshold int) bool {
	//remove to prevent double locking and deadlocks
	/*b.bankLock.Lock()
	account := b.accounts[accountId]
	b.bankLock.Unlock()

	account.lock.Lock()
	defer account.lock.Unlock()
	*/
	var compareResult bool
	b.Deposit(accountId, amount)
	compareResult = b.GetBalance(accountId) >= compareThreshold

	// return compared result
	return compareResult
}

// returns the balance of the given account id
func (b *Bank) GetBalance(accountID int) int {
	account := b.accounts[accountID]
	account.lock.Lock()
	defer account.lock.Unlock()
	return account.balance
}
