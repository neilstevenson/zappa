package neil.demo.zappa.panel;

import java.time.LocalDate;

import javax.swing.JFrame;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryUpdatedListener;

import neil.demo.zappa.MyConstants;
import neil.demo.zappa.TimePrice;

/**
 * <p>This class listens for changes to the
 * "{@code price}" {@link com.hazelcast.core.IMap IMap} and passes them
 * to the {@link PricePanel} which is a panel displaying a scrolling
 * graph of the current BTC/USD price and the moving averages.
 * </p>
 */
public class PricePanelListener implements EntryUpdatedListener<String, TimePrice> {

	private static PricePanel pricePanel = null;

	/**
	 * <p>Create and display a panel as the only visible
	 * component in a frame.
	 * </p>
	 */
	public void activateDisplay() {
		pricePanel = new PricePanel();

		JFrame frame = new JFrame(MyConstants.PANEL_TITLE);

		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.add(pricePanel);
		frame.pack();
		frame.setVisible(true);
	}

	/**
	 * <p>On an update event, the save to the Hazelcast map,
	 * pass the new value to the chart to display.
	 * </p>
	 *
	 * @param entryEvent An update to "{@code price}" {@link com.hazelcast.core.IMap IMap}
	 */
	@Override
	public void entryUpdated(EntryEvent<String, TimePrice> entryEvent) {

		// Initialise the panel if needed
		synchronized (this) {
			if (pricePanel == null) {
				this.activateDisplay();
			}
		}
		
		TimePrice timePrice = entryEvent.getValue();

		LocalDate day = TimePrice.convert(timePrice.getDate());
		double rate = timePrice.getRate().doubleValue();
		
		pricePanel.update(entryEvent.getKey(), rate, day);
	}

}
