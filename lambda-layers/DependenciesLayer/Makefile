build-DependenciesLayer:
	pip install --upgrade pip
	pip install --upgrade -r requirements.txt --target "$(ARTIFACTS_DIR)/python/" --log /dev/null
	cp requirements.txt "$(ARTIFACTS_DIR)/python/installed-requirements.txt"
