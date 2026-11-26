package gdelt

// Định dạng của GDELT DOC API, mode=artlist&format=json.
type artListResponse struct {
	Articles []article `json:"articles"`
}

type article struct {
	URL           string `json:"url"`
	Title         string `json:"title"`
	SeenDate      string `json:"seendate"` // "20261126T043000Z"
	Domain        string `json:"domain"`
	Language      string `json:"language"` // "English"
	SourceCountry string `json:"sourcecountry"`
}

const seenDateLayout = "20060102T150405Z"
