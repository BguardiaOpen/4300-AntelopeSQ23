// Writing this as a separate class because using the STL stack
// directly gives an "undefined reference" error. But this doesn't fix it
using namespace std;
#include <stack>

class TransactionStack{
    private:
        std::stack<int> transactionStack;
    public:
        TransactionStack(){
            transactionStack = stack<int>();
        }

        void push(int i){
            transactionStack.push(i);
        }

        void pop(){
            transactionStack.pop();
        }

        int size(){ return transactionStack.size(); }

        bool empty(){ return transactionStack.empty(); }
};