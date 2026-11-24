function progress(done, step, spinner, filled, i, bar) {
	if (started > total) total = started
	filled = int(20 * done / total)
	for (i = 0; i < 20; i++) bar = bar (i < filled ? "#" : "-")
	printf "\r\033[K[%s] %d/%d %s%s", bar, done, total, step, spinner
	fflush()
}

function running() {
	progress(started ? started - 1 : 0, step, " " substr("|/-\\", frame % 4 + 1, 1))
}

BEGIN {
	status = 1
	row = "%-30s | %10s | %11s | %12s | %16s | %14s\n"
	step = "Starting tinyq benchmarks..."
	if (terminal) running()
	else print "Running tinyq benchmarks..."
	fflush()
}

/^tinyq-tick$/ {
	frame++
	running()
	next
}

/^Benchmark/ && NF == 1 {
	if (index($1, "/")) {
		started++
		step = "Running " substr($1, 10)
		if (terminal) running()
	}
	next
}

/^Benchmark/ && $4 == "ns/op" && $6 == "B/op" && $8 == "allocs/op" {
	if (!rows++) report = report "\n" sprintf(row, "Benchmark", "Go CPUs", "Iterations", "Time (ns/op)", "Allocated (B/op)", "Allocations/op")
	name = substr($1, 10); cpus = 1
	if (match(name, /-[0-9]+$/)) {
		cpus = substr(name, RSTART + 1)
		name = substr(name, 1, RSTART - 1)
	}
	report = report sprintf(row, name, cpus, $2, $3, $5, $7)
	next
}

/^tinyq-exit / { status = $2; next }
/^PASS$/ { report = report "\n" }
{ report = report $0 "\n" }

END {
	if (terminal) {
		if (status == 0 && started) {
			total = started
			progress(started, "Complete")
			print ""
		} else printf "\r\033[K"
	}
	printf "%s", report
	exit status
}
