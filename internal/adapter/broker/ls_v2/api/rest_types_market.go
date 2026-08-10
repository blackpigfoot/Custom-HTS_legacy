package api


const (
	// PathStockMarket is the REST path for market-data TRs.
	PathStockMarket = "/stock/market-data"
)

const (
	TrIDStockPriceByMinute = "t1302" // LS t1302 TR code for minute-by-minute price lookup.
)


// t1101Request is the native request body for t1101.
type t1101Request struct {
	In t1101InBlock `json:"t1101InBlock"` // Input block.
}

// t1101InBlock is the native input block for t1101.
type t1101InBlock struct {
	Shcode string `json:"shcode"` // Short issue code.
}

// t1102Request is the native request body for t1102.
type t1102Request struct {
	In t1102InBlock `json:"t1102InBlock"` // Input block.
}

// t1102InBlock is the native input block for t1102.
type t1102InBlock struct {
	Shcode string `json:"shcode"` // Short issue code.
}

// t8407Request is the native request body for t8407.
type t8407Request struct {
	In t8407InBlock `json:"t8407InBlock"` // Input block.
}

// t8407InBlock is the native input block for t8407.
type t8407InBlock struct {
	Nrec   int    `json:"nrec"`   // Number of requested codes.
	Shcode string `json:"shcode"` // Concatenated short-code list without delimiters.
}

// t1405Request is the native request body for t1405.
type t1405Request struct {
	In t1405InBlock `json:"t1405InBlock"` // Input block.
}

// t1405InBlock is the native input block for t1405.
type t1405InBlock struct {
	Gubun      string `json:"gubun"`      // Market type.
	Jongchk    string `json:"jongchk"`    // Warning type.
	Cts_shcode string `json:"cts_shcode"` // Continuation short code for next query.
}

type t1302Request struct {
	In t1302InBlock `json:"t1302InBlock"` // Input block.
}

type t1302InBlock struct {
	Shcode    string `json:"shcode"`    // Short issue code.
	Gubun     string `json:"gubun"`     // Minute type.
	Time      string `json:"time"`      // First query: empty string. Continuation query: cts_time from the previous response.
	Cnt       int    `json:"cnt"`       // Query Count(1 ~ 900)
	Exchgubun string `json:"exchgubun"` // Exchange type (U: All, K: KRX, N: NXT)
}

type t1302Response struct {
	baseResponse
	T1302OutBlock  t1302OutBlock    `json:"t1302OutBlock"`  // Summary information block.
	T1302OutBlock1 []t1302OutBlock1 `json:"t1302OutBlock1"` // Minute price list block.
}

type t1302OutBlock struct {
	Cts_Time string `json:"cts_time"` // Continuation time for next query.
}

type t1302OutBlock1 struct {
	Chetime     string       `json:"chetime"`     // Time in HHMMSS format.
	Close       int64        `json:"close"`       // Price at the end of the minute.
	Sign        string       `json:"sign"`        // Price change sign code.
	Change      int64        `json:"change"`      // Price change from previous minute.
	Diff        DecimalValue `json:"diff"`        // Price change rate.
	Chdegree    DecimalValue `json:"chdegree"`    // Price change degree text.
	Mdvolume    int64        `json:"mdvolume"`    // Trading volume during the minute.
	Msvolume    int64        `json:"msvolume"`    // Trading volume from the previous minute to the current minute.
	Revolume    int64        `json:"revolume"`    // Trading volume from the current minute to the next minute.
	Mdchecnt    int64        `json:"mdchecnt"`    // Number of trades during the minute.
	Mschecnt    int64        `json:"mschecnt"`    // Number of trades from the previous minute to the current minute.
	Rechecnt    int64        `json:"rechecnt"`    // Number of trades from the current minute to the next minute.
	Volume      int64        `json:"volume"`      // Cumulative trading volume.
	Open        int64        `json:"open"`        // Opening price of the minute.
	High        int64        `json:"high"`        // High price of the minute.
	Low         int64        `json:"low"`         // Low price of the minute.
	Cvolume     int64        `json:"cvolume"`     // Cumulative trading volume from the start of the day to the end of the minute.
	Mdchecnttm  int64        `json:"mdchecnttm"`  // Cumulative number of trades from the start of the day to the end of the minute.
	Mschecnttm  int64        `json:"mschecnttm"`  // Cumulative number of trades from the start of the day to the end of the minute.
	Totofferrem int64        `json:"totofferrem"` // Total ask quantity.
	Totbidrem   int64        `json:"totbidrem"`   // Total bid quantity.
	Mdvolumetm  int64        `json:"mdvolumetm"`  // Cumulative trading volume from the start of the day to the end of the minute.
	Msvolumetm  int64        `json:"msvolumetm"`  // Cumulative trading volume from the start of the day to the end of the minute.
}

// T1405Response is the native LS response DTO for t1405.
type T1405Response struct {
	baseResponse
	T1405OutBlock  t1405OutBlock    `json:"t1405OutBlock"`  // Summary information block.
	T1405OutBlock1 []t1405OutBlock1 `json:"t1405OutBlock1"` // Warning list block.
}

// t1405OutBlock keeps the vendor-native t1405 summary block intact.
type t1405OutBlock struct {
	Cts_shcode string `json:"cts_shcode"` // Continuation short code for next query.
}

// t1405OutBlock1 keeps the vendor-native t1405 warning row intact.
type t1405OutBlock1 struct {
	Hname  string       `json:"hname"`  // Stock Korean name.
	Price  int64        `json:"price"`  // Current price.
	Sign   string       `json:"sign"`   // Price change sign code.
	Change int64        `json:"change"` // Price change from previous close.
	Diff   DecimalValue `json:"diff"`   // Price change rate text.
	Volume int64        `json:"volume"` // Trading volume.
	Date   string       `json:"date"`   // Warning date in YYYYMMDD format.
	Edate  string       `json:"edate"`  // Warning lifting date in YYYYMMDD format.
	Shcode string       `json:"shcode"` // Short issue code.
}

// T1102Response is the native LS response DTO for t1102.
type T1102Response struct {
	RspCd    string        `json:"rsp_cd"`        // Response code.
	RspMsg   string        `json:"rsp_msg"`       // Response message.
	OutBlock T1102OutBlock `json:"t1102OutBlock"` // Price output block.
}

// T1102OutBlock keeps the vendor-native t1102 payload intact.
type T1102OutBlock struct {
	Hname       string       `json:"hname"`       // Issue name.
	Price       int64        `json:"price"`       // Current price.
	Sign        string       `json:"sign"`        // Change sign code.
	Change      int64        `json:"change"`      // Change from previous close.
	Diff        DecimalValue `json:"diff"`        // Change rate text.
	Volume      int64        `json:"volume"`      // Cumulative volume.
	RecPrice    int64        `json:"recprice"`    // Reference price.
	Avg         int64        `json:"avg"`         // Weighted average price.
	JnilClose   int64        `json:"jnilclose"`   // Previous close.
	OfferHo     int64        `json:"offerho"`     // Best ask price.
	BidHo       int64        `json:"bidho"`       // Best bid price.
	OfferRem    int64        `json:"offerrem"`    // Best ask quantity.
	BidRem      int64        `json:"bidrem"`      // Best bid quantity.
	PreOfferCha int64        `json:"preoffercha"` // Ask quantity delta from the previous snapshot.
	PreBidCha   int64        `json:"prebidcha"`   // Bid quantity delta from the previous snapshot.
	Open        int64        `json:"open"`        // Opening price.
	High        int64        `json:"high"`        // Session high.
	Low         int64        `json:"low"`         // Session low.
	HoStatus    string       `json:"ho_status"`   // Quote status.
	Hotime      string       `json:"hotime"`      // Quote time.
	YePrice     int64        `json:"yeprice"`     // Expected match price.
	YeVolume    int64        `json:"yevolume"`    // Expected match volume.
	YeSign      string       `json:"yesign"`      // Expected match sign code.
	YeChange    int64        `json:"yechange"`    // Expected match change.
	YeDiff      DecimalValue `json:"yediff"`      // Expected match change rate text.
	TmOffer     int64        `json:"tmoffer"`     // After-hours ask quantity.
	TmBid       int64        `json:"tmbid"`       // After-hours bid quantity.
	Shcode      string       `json:"shcode"`      // Short issue code.
	UplmtPrice  int64        `json:"uplmtprice"`  // Upper limit price.
	DnlmtPrice  int64        `json:"dnlmtprice"`  // Lower limit price.
	Value       int64        `json:"value"`       // Cumulative traded amount.
	MarketCap   int64        `json:"marketcap"`   // Market capitalization.
}

// T1101Response is the native LS response DTO for t1101.
type T1101Response struct {
	RspCd    string        `json:"rsp_cd"`        // Response code.
	RspMsg   string        `json:"rsp_msg"`       // Response message.
	OutBlock T1101OutBlock `json:"t1101OutBlock"` // Orderbook output block.
}

// T1101OutBlock keeps the vendor-native t1101 orderbook payload intact.
type T1101OutBlock struct {
	Hname      string       `json:"hname"`         // Issue name.
	Price      int64        `json:"price"`         // Current price.
	Sign       string       `json:"sign"`          // Change sign code.
	Change     int64        `json:"change"`        // Change from previous close.
	Diff       DecimalValue `json:"diff"`          // Change rate text.
	Volume     int64        `json:"volume"`        // Cumulative volume.
	JnilClose  int64        `json:"jnilclose"`     // Previous close.
	OfferHo1   int64        `json:"offerho1"`      // Ask price level 1.
	BidHo1     int64        `json:"bidho1"`        // Bid price level 1.
	OfferRem1  int64        `json:"offerrem1"`     // Ask quantity level 1.
	BidRem1    int64        `json:"bidrem1"`       // Bid quantity level 1.
	PreOffer1  int64        `json:"preoffercha1"`  // Ask quantity delta level 1.
	PreBid1    int64        `json:"prebidcha1"`    // Bid quantity delta level 1.
	OfferHo2   int64        `json:"offerho2"`      // Ask price level 2.
	BidHo2     int64        `json:"bidho2"`        // Bid price level 2.
	OfferRem2  int64        `json:"offerrem2"`     // Ask quantity level 2.
	BidRem2    int64        `json:"bidrem2"`       // Bid quantity level 2.
	PreOffer2  int64        `json:"preoffercha2"`  // Ask quantity delta level 2.
	PreBid2    int64        `json:"prebidcha2"`    // Bid quantity delta level 2.
	OfferHo3   int64        `json:"offerho3"`      // Ask price level 3.
	BidHo3     int64        `json:"bidho3"`        // Bid price level 3.
	OfferRem3  int64        `json:"offerrem3"`     // Ask quantity level 3.
	BidRem3    int64        `json:"bidrem3"`       // Bid quantity level 3.
	PreOffer3  int64        `json:"preoffercha3"`  // Ask quantity delta level 3.
	PreBid3    int64        `json:"prebidcha3"`    // Bid quantity delta level 3.
	OfferHo4   int64        `json:"offerho4"`      // Ask price level 4.
	BidHo4     int64        `json:"bidho4"`        // Bid price level 4.
	OfferRem4  int64        `json:"offerrem4"`     // Ask quantity level 4.
	BidRem4    int64        `json:"bidrem4"`       // Bid quantity level 4.
	PreOffer4  int64        `json:"preoffercha4"`  // Ask quantity delta level 4.
	PreBid4    int64        `json:"prebidcha4"`    // Bid quantity delta level 4.
	OfferHo5   int64        `json:"offerho5"`      // Ask price level 5.
	BidHo5     int64        `json:"bidho5"`        // Bid price level 5.
	OfferRem5  int64        `json:"offerrem5"`     // Ask quantity level 5.
	BidRem5    int64        `json:"bidrem5"`       // Bid quantity level 5.
	PreOffer5  int64        `json:"preoffercha5"`  // Ask quantity delta level 5.
	PreBid5    int64        `json:"prebidcha5"`    // Bid quantity delta level 5.
	OfferHo6   int64        `json:"offerho6"`      // Ask price level 6.
	BidHo6     int64        `json:"bidho6"`        // Bid price level 6.
	OfferRem6  int64        `json:"offerrem6"`     // Ask quantity level 6.
	BidRem6    int64        `json:"bidrem6"`       // Bid quantity level 6.
	PreOffer6  int64        `json:"preoffercha6"`  // Ask quantity delta level 6.
	PreBid6    int64        `json:"prebidcha6"`    // Bid quantity delta level 6.
	OfferHo7   int64        `json:"offerho7"`      // Ask price level 7.
	BidHo7     int64        `json:"bidho7"`        // Bid price level 7.
	OfferRem7  int64        `json:"offerrem7"`     // Ask quantity level 7.
	BidRem7    int64        `json:"bidrem7"`       // Bid quantity level 7.
	PreOffer7  int64        `json:"preoffercha7"`  // Ask quantity delta level 7.
	PreBid7    int64        `json:"prebidcha7"`    // Bid quantity delta level 7.
	OfferHo8   int64        `json:"offerho8"`      // Ask price level 8.
	BidHo8     int64        `json:"bidho8"`        // Bid price level 8.
	OfferRem8  int64        `json:"offerrem8"`     // Ask quantity level 8.
	BidRem8    int64        `json:"bidrem8"`       // Bid quantity level 8.
	PreOffer8  int64        `json:"preoffercha8"`  // Ask quantity delta level 8.
	PreBid8    int64        `json:"prebidcha8"`    // Bid quantity delta level 8.
	OfferHo9   int64        `json:"offerho9"`      // Ask price level 9.
	BidHo9     int64        `json:"bidho9"`        // Bid price level 9.
	OfferRem9  int64        `json:"offerrem9"`     // Ask quantity level 9.
	BidRem9    int64        `json:"bidrem9"`       // Bid quantity level 9.
	PreOffer9  int64        `json:"preoffercha9"`  // Ask quantity delta level 9.
	PreBid9    int64        `json:"prebidcha9"`    // Bid quantity delta level 9.
	OfferHo10  int64        `json:"offerho10"`     // Ask price level 10.
	BidHo10    int64        `json:"bidho10"`       // Bid price level 10.
	OfferRem10 int64        `json:"offerrem10"`    // Ask quantity level 10.
	BidRem10   int64        `json:"bidrem10"`      // Bid quantity level 10.
	PreOffer10 int64        `json:"preoffercha10"` // Ask quantity delta level 10.
	PreBid10   int64        `json:"prebidcha10"`   // Bid quantity delta level 10.
	Offer      int64        `json:"offer"`         // Total ask quantity.
	Bid        int64        `json:"bid"`           // Total bid quantity.
	PreOffer   int64        `json:"preoffercha"`   // Total ask quantity delta.
	PreBid     int64        `json:"prebidcha"`     // Total bid quantity delta.
	Hotime     string       `json:"hotime"`        // Quote time.
	YePrice    int64        `json:"yeprice"`       // Expected match price.
	YeVolume   int64        `json:"yevolume"`      // Expected match volume.
	YeSign     string       `json:"yesign"`        // Expected match sign code.
	YeChange   int64        `json:"yechange"`      // Expected match change.
	YeDiff     DecimalValue `json:"yediff"`        // Expected match change rate text.
	TmOffer    int64        `json:"tmoffer"`       // After-hours ask quantity.
	TmBid      int64        `json:"tmbid"`         // After-hours bid quantity.
	HoStatus   string       `json:"ho_status"`     // Quote status.
	Shcode     string       `json:"shcode"`        // Short issue code.
	UplmtPrice int64        `json:"uplmtprice"`    // Upper limit price.
	DnlmtPrice int64        `json:"dnlmtprice"`    // Lower limit price.
	Open       int64        `json:"open"`          // Opening price.
	High       int64        `json:"high"`          // Session high.
	Low        int64        `json:"low"`           // Session low.
}

// T8407Response is the native LS response DTO for t8407.
type T8407Response struct {
	RspCd     string           `json:"rsp_cd"`         // Response code.
	RspMsg    string           `json:"rsp_msg"`        // Response message.
	OutBlock1 []T8407OutBlock1 `json:"t8407OutBlock1"` // Multi-price snapshot list.
}

// T8407OutBlock1 keeps the vendor-native t8407 snapshot payload intact.
type T8407OutBlock1 struct {
	Shcode      string       `json:"shcode"`      // Short issue code.
	Hname       string       `json:"hname"`       // Issue name.
	Price       int64        `json:"price"`       // Current price.
	Sign        string       `json:"sign"`        // Change sign code.
	Change      int64        `json:"change"`      // Change from previous close.
	Diff        DecimalValue `json:"diff"`        // Change rate text.
	Volume      int64        `json:"volume"`      // Cumulative volume.
	OfferHo     int64        `json:"offerho"`     // Best ask price.
	BidHo       int64        `json:"bidho"`       // Best bid price.
	Cvolume     int64        `json:"cvolume"`     // Trade volume.
	Chdegree    DecimalValue `json:"chdegree"`    // Trade strength text.
	Open        int64        `json:"open"`        // Opening price.
	High        int64        `json:"high"`        // Session high.
	Low         int64        `json:"low"`         // Session low.
	Value       int64        `json:"value"`       // Cumulative traded amount.
	OfferRem    int64        `json:"offerrem"`    // Best ask quantity.
	BidRem      int64        `json:"bidrem"`      // Best bid quantity.
	TotOfferRem int64        `json:"totofferrem"` // Total ask quantity.
	TotBidRem   int64        `json:"totbidrem"`   // Total bid quantity.
	JnilClose   int64        `json:"jnilclose"`   // Previous close.
	UplmtPrice  int64        `json:"uplmtprice"`  // Upper limit price.
	DnlmtPrice  int64        `json:"dnlmtprice"`  // Lower limit price.
}

// CheckError reports the LS business error embedded in t1102.
func (r *T1102Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t1101.
func (r *T1101Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t8407.
func (r *T8407Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t1405.
func (r *T1405Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}
