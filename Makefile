.DEFAULT_GOAL := help

.PHONY: all
all: clean requirements.txt

.PHONY: clean
clean: ## Delete requirements.txt
	rm -rf requirements.txt

requirements.txt: Pipfile Pipfile.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	pipenv lock -r > requirements.txt

.PHONY: help
# gratuitously adapted from https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Display this help text
	@grep -E '^[-a-zA-Z0-9_/.]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m\n\t%s\n", $$1, $$2}'
