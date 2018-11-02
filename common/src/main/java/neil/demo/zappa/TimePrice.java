package neil.demo.zappa;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import lombok.Data;

/**
 * <p>Represents a price at a particular point in time.
 * </p>
 */
@SuppressWarnings("serial")
@Data
public class TimePrice implements Serializable {

	private Date date;
	private BigDecimal rate;

	/**
	 * <p>Convenience constructor if passed a {@link java.time.LocalDate}
	 * </p>
	 * 
	 * @param localDate
	 * @param rate
	 */
	public TimePrice(LocalDate localDate, BigDecimal rate) {
		this.date = TimePrice.convert(localDate);
		this.rate = rate;
	}

	/**
	 * <p>TODO: In-convenience constructor! Lombok's {@code @AllArgsConstructor}
	 * should handle this, but something fails somewhere.
	 * </p>
	 * 
	 * @param date
	 * @param rate
	 */
	public TimePrice(Date date, BigDecimal rate) {
		this.date = date;
		this.rate = rate;
	}
	
	/**
	 * <p>For windowing, more convenient to have the date as a
	 * number.
	 * </p>
	 * 
	 * @return
	 */
	public long getTimestamp() {
		return this.getDate().getTime();
	}
	
	/**
	 * <p>Convenience method to turn a {@link java.time.LocalDate LocalDate}
	 * into {@link java.util.Date Date}.
	 * </p>
	 */
	public static Date convert(LocalDate localDate) {
		return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
	}
	/**
	 * <p>Convenience method to turn a {@link java.time.LocalDate LocalDate}
	 * into {@link java.util.Date Date}.
	 * </p>
	 */
	public static LocalDate convert(Date date) {
		return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
	}

}
