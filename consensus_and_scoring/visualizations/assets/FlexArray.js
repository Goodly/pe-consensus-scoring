class FlexArray { // an array that keeps order of objects and adjusts itself to removal of objects
  // functions only support the objects being arrays
  constructor() { this.array = []; }

  get(index) { return this.array[index]; }

  getArray() { return this.array; }

  push(addArr) { this.array.push(addArr);}

  getSize() { return this.array.length; }

  remove(removeArr) {
    this.array = this.array.filter(function(ele){ return removeArr[0] != ele[0]; });
  }
}

function highlightSort(h1, h2) { // helper function to sortJSONentries. Sorts arrays by their "index" value (contained in array[2]).
	if (h1[2] > h2[2]) {
		return 1;
	} else if (h1[2] < h2[2]) {
		return -1;
	}
	 return 0;
}
