.PHONY: run bench help

run:
	go run ./cmd/tinyq

bench:
	@terminal=0; \
	if [ -t 1 ] && [ "$$TERM" != dumb ]; then \
		terminal=1; trap 'printf "\r\033[K"; exit 130' HUP INT TERM; \
	fi; \
	{ \
		stop_ticker() { \
			if [ -n "$$ticker" ]; then kill "$$ticker" 2>/dev/null; wait "$$ticker" 2>/dev/null; fi; \
		}; \
		ticker=; \
		if [ "$$terminal" -eq 1 ]; then \
			(while sleep 0.15; do printf 'tinyq-tick\n' || break; done) & ticker=$$!; \
			trap 'stop_ticker; exit 130' HUP INT TERM; \
		fi; \
		{ go test ./internal/queue -run '^$$' -v -bench . -benchmem 2>&1; printf 'tinyq-exit %s\n' "$$?"; } | \
			while IFS= read -r line; do printf '%s\n' "$$line"; done; \
		stop_ticker; \
	} | \
		awk -v terminal=$$terminal -v total=17 -f scripts/bench.awk

help:
	@printf '%s\n' 'make run   Start tinyq' 'make bench Show benchmark report' 'make help  Show this help'
