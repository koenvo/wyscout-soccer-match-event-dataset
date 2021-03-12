# Soccer match event dataset - processed

This repository contains the Wyscout data described in the 'A public data set of spatio-temporal match events in soccer competitions' paper, but processed to the regular Wyscout form. In this form it can be loaded by libraries like [kloppy](https://kloppy.pysport.org).

The dataset contains the following competitions:

| Competition | Number of games | Season |
|-------------|-----------------|--------| 
| French Ligue 1 | 380 | 2017/18 |
| English Premier League | 380 | 2017/18 |
| Italian Serie A | 380 | 2017/18
| Spanish La Liga | 380 | 2017/18
| German Bundesliga | 306 | 2017/18
| Men’s FIFA World Cup | 64 | 2018
| Men’s UEFA Euro | 51 | 2016
Andrew Rowlinson (2020): [FOOTBALL SHOT QUALITY](https://aaltodoc.aalto.fi/bitstream/handle/123456789/45953/master_Rowlinson_Andrew_2020.pdf?sequence=1&isAllowed=y)

## Usage

The original files are processed to the regular JSON form. This makes it possible to load the data using kloppy. Since kloppy 1.5.2 it's possible to access this dataset directly using the kloppy `datasets` api.

You can find all available matches in the [index](processed/README.md) in the repository.


### Load using datasets api

Note: make sure you have version 1.5.2 of kloppy.

You can load the default match (2499841 - Huddersfield Town - Manchester City) using the dataset api:

```python   
from kloppy import datasets
dataset = datasets.load("wyscout")
```

If you want to use any other match you have to specify the match_id:


```python   
from kloppy import datasets
dataset = datasets.load("wyscout", match_id=2499843)
```


### Local file

```shell script
wget https://raw.githubusercontent.com/koenvo/wyscout-soccer-match-event-dataset/main/processed/1694390.json

```
```python
from kloppy import load_wyscout_event_data
dataset = load_wyscout_event_data("1694390.json")
```

## Sources

| Type | Format | Url |
|------|--------|-----|
| Players | JSON | https://ndownloader.figshare.com/files/15073721 |
| Teams | JSON | https://ndownloader.figshare.com/files/15073697 |
| Matches | Zipped JSON | https://ndownloader.figshare.com/files/14464622 |
| Events | Zipped JSON | https://ndownloader.figshare.com/files/14464685 |  


## References

Pappalardo, Luca; Massucco, Emanuele (2019): Soccer match event dataset. figshare. Collection. https://doi.org/10.6084/m9.figshare.c.4415000

Pappalardo, L., Cintia, P., Rossi, A. et al. A public data set of spatio-temporal match events in soccer competitions. Sci Data 6, 236 (2019). https://doi.org/10.1038/s41597-019-0247-7

Data source: publicly available on [https://figshare.com/collections/Soccer_match_event_dataset/4415000/2](https://figshare.com/collections/Soccer_match_event_dataset/4415000/2)

## License

The data sets are released under the CC BY 4.0 License.