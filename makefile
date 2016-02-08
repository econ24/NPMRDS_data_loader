info:
	@echo clean: deletes all junk files
	@echo move: moves all formatted .CSV files into Northeast directory

clean:
	@rm -rf FHWA*
	@rm -rf ./Northeast/FHWA*

move:
	@mv formatted* ./Northeast/