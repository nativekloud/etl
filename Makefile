build:
	lein bin
msgraph:
	target/etl tap --type msgraph | target/etl transform --type merge > results.json
