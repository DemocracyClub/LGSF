.DEFAULT_GOAL := help

.PHONY: all
all: clean lambda-layers/DependenciesLayer/requirements.txt

.PHONY: clean
clean: ## Delete lambda-layers/DependenciesLayer/requirements.txt
	rm -rf lambda-layers/DependenciesLayer/requirements.txt

lambda-layers/DependenciesLayer/requirements.txt: uv.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	mkdir -p lambda-layers/DependenciesLayer
	# Playwright is only used by the scraper worker, whose Docker image installs
	# its own dependencies. It's over half the size of this layer, which has to
	# fit within Lambda's 250MB limit along with the function code.
	uv export --no-dev --no-emit-package playwright --no-emit-package greenlet --no-emit-package pyee > lambda-layers/DependenciesLayer/requirements.txt
