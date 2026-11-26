package jetstream

type event struct {
	TimeUS int64   `json:"time_us"`
	Kind   string  `json:"kind"`
	Commit *commit `json:"commit"`
}

type commit struct {
	Operation  string  `json:"operation"`
	Collection string  `json:"collection"`
	Record     *record `json:"record"`
}

type record struct {
	Text  string   `json:"text"`
	Langs []string `json:"langs"`
}
