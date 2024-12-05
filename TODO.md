# TODO for basic functionality
* open/create database
* How are buckets stored at runtime?
* ~~How does the mut overlay work? (BTreeMap)~~
* Where is the delineation between the backend and front-end?
* Loose transaction
* Backend documentation
* Double-check bbolt compatibility 
  * Make compatibility a runtime option, not compile time
* PageBuffer vs. BucketBuffer? How do I clean it up? 
* ~~get functions vs. pub everything?~~
* Memory backend
* MMap backend
* File Backend
* ~~Free page assignment? Can't be any worse than what we already do~~
* 

Version 3:
* NodePageId (page id) is mapped to the physical disk page
  * Replace FreeListPage with PageMapPage
  * Map<PageId, DiskPageId> in the fetch page
* Few item pages (<16?) may not benefit from multithreading.
  * Maybe a special page for that more cache friendly searching algorithm would work?
* Compaction ideas
  * https://ieeexplore.ieee.org/document/10102447