package neil.demo.zappa.jet.account;

import java.util.List;
import java.util.Map.Entry;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.AbstractEntryProcessor;

import neil.demo.zappa.AccountBaseline;
import neil.demo.zappa.AccountTransaction;

/**
 * <p>Merge an {@link AccountTransaction} into a record with an
 * {@link AccountBaseline} by merely appending to the transaction
 * list and adjusting the value.
 * </p>
 */
@SuppressWarnings("serial")
public class AccountMergeEntryProcessor extends AbstractEntryProcessor<String, Tuple2<AccountBaseline, List<AccountTransaction>>> {
	
	private AccountTransaction accountTransaction;
	
	AccountMergeEntryProcessor(Object arg0) {
		this.accountTransaction = (AccountTransaction) arg0;
	}

	/**
	 * <p>Append the transaction to this list
	 * </p>
	 */
	@Override
	public Void process(Entry<String, Tuple2<AccountBaseline, List<AccountTransaction>>> entry) {
		Tuple2<AccountBaseline, List<AccountTransaction>> value = entry.getValue();
			
		if (value!=null) {
			AccountBaseline baseline = value.getKey();
			List<AccountTransaction> transactions = value.getValue();
			
			// Append to the list
			transactions.add(this.accountTransaction);
			
			// Change the last amendment date
			baseline.setWhen(this.accountTransaction.getWhen());
			
			// Adjust the balance
			double balance = baseline.getBalance();
			balance = this.accountTransaction.getDebit() ?
					balance - this.accountTransaction.getAmount() :
					balance + this.accountTransaction.getAmount();
			baseline.setBalance(balance);
			
			entry.setValue(value);
		}
		
		return null;
	}

}
