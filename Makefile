.DEFAULT_GOAL := help

.PHONY: all
all: clean lambda-layers/DependenciesLayer/requirements.txt

.PHONY: clean
clean: ## Delete lambda-layers/DependenciesLayer/requirements.txt
	rm -rf lambda-layers/DependenciesLayer/requirements.txt

lambda-layers/DependenciesLayer/requirements.txt: uv.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	uv export --no-dev > lambda-layers/DependenciesLayer/requirements.txt

requirements.txt: uv.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	uv export --no-dev > requirements.txt

.PHONY: help
# gratuitously adapted from https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Display this help text
	@grep -E '^[-a-zA-Z0-9_/.]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m\n\t%s\n", $$1, $$2}'
