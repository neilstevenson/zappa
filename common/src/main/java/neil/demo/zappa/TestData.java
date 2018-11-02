package neil.demo.zappa;

public class TestData {

	// First Folio (1623)
	public static final String[] HAMLET = new String[] { 
		"To be, or not to be, that is the Question:",
		"Whether ’tis Nobler in the mind to suffer",
		"The Slings and Arrows of outragious Fortune,",
		"Or to take Armes against a Sea of troubles,",
		"And by opposing end them: to dye, to ſleepe",
		"No more; and by a sleep, to say we end",
		"The Heart-ake, and the thouſand Naturall ſhockes",
		"That Flesh is heyre too? 'Tis a consummation",
		"Deuoutly to be wiſh'd. To dye to sleepe,",
		"To sleep, perchance to Dream; I, there's the rub,",
		"For in that sleep of death, what dreams may come,",
		"When we haue ſhufflel’d off this mortall coile,",
		"Muſt giue us pause. There's the respect",
		"That makes Calamity of long life:",
		"For who would beare the Whips and Scornes of time,",
		"The Oppreſſors wrong, the poore mans Contumely,",
		"The pangs of diſpriz’d Loue, the Lawes delay,",
		"The inſolence of Office, and the Spurnes",
		"That patient merit of the unworthy takes,",
		"When he himſelfe might his Quietus make",
		"With a bare Bodkin? Who would theſe Fardles beare",
		"To grunt and ſweat vnder a weary life,",
		"But that the dread of ſomething after death,",
		"The vndiſcouered Countrey, from whoſe Borne",
		"No Traueller returnes, Puzels the will,",
		"And makes vs rather beare those illes we haue,",
		"Then flye to others that we know not of.",
		"Thus Conſcience does make Cowards of vs all,",
		"And thus the Natiue hew of Resolution",
		"Is ſicklied o’re, with the pale caſt of Thought,",
		"And enterprizes of great pith and moment,",
		"With this regard their Currants turne away,",
		"And looſe the name of Action. Soft you now,",
		"The faire Ophelia? Nimph, in thy Orizons",
		"Be all my ſinnes remembred.",
	};

	// Account, perhaps should use Integer instead of String for key
	public static final String[][] ACCOUNT_BASELINE = new String[][] { 
			{ "1" , "Neil",   "2018-01-01", "100.00" },
			{ "2" , "Martin", "2018-01-01", "-100.00" },
			{ "3" , "Jonny",  "2018-01-01", "0.00" },
			{ "4" , "Roger",  "2018-01-01", "0.00" },
	};
	public static final String[][] ACCOUNT_TRANSACTIONS_1 = new String[][] { 
		{ "1" , "2018-10-31T23:59", "100.00", "Salary" },
		{ "2" , "2018-10-31T23:59", "100.00", "Salary" },
		{ "3" , "2018-10-31T23:59", "100.00", "Salary" },
		{ "1" , "2018-11-07T18:40", "-25.00", "Heathrow Express" },
		{ "2" , "2018-11-07T18:40", "-25.00", "Heathrow Express" },
		{ "1" , "2018-11-07T18:42", "-2.00", "Starbucks" },
	};
	public static final String[][] ACCOUNT_TRANSACTIONS_2 = new String[][] { 
		{ "1" , "2018-11-07T19:00", "3.00", "Interest" },
		{ "1" , "2018-11-07T19:30", "-3.95", "Weatherspoons" },
		{ "1" , "2018-11-07T19:35", "-3.95", "Weatherspoons" },
		{ "1" , "2018-11-07T19:40", "-3.95", "Weatherspoons" },
	};
}
