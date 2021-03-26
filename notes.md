## 2020-12-13
[Flo]
* We probably need some kind of permissions? (`CRUDE`-like probably with some additional like `UPDATE_ORIGINAL`, `UPDATE_DERIVED_ONLY`)
* 

## 2020-12-12
[Flo]
* The intended design is straight forward. I have it clear in my mind.
* I'm just thinking that maybe a Web-based API could add bookkeeping complexity
* API's definition is an abstract class under `ds_catalog_service.api.base.Catalog`
* Since the API is not 100% clear yet, I'll probably focus on a CLI-based implementation for now (under `ds_catalog_service.api.cli`)
and run experiments on top of that.
* Plus, CLI-based APIs are always easier to setup :-) . 
* A second concrete web-based implementation is always easier than the first one.

## 2020-12-8
[Flo]
* Instead of polluting with endless `how-to` notes and `.sh` scripts; meaningful