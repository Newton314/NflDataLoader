from NflDataLoader.dataloader import NflLoader


SEASONS = (2014, 2013)
LOADER = NflLoader(new=True, save=True)
for season in SEASONS:
    _ = LOADER.get_seasontable(season)
