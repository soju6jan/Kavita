using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using API.Data;
using API.Data.Metadata;
using API.Data.Repositories;
using API.Entities;
using API.Entities.Enums;
using API.Extensions;
using API.Helpers;
using API.Helpers.Builders;
using API.Services.Plus;
using API.Services.Tasks.Metadata;
using API.Services.Tasks.Scanner.Parser;
using API.SignalR;
using Kavita.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.IO;


namespace API.Services.Tasks.Scanner;
#nullable enable

public interface IProcessSeries
{
    /// <summary>
    /// Do not allow this Prime to be invoked by multiple threads. It will break the DB.
    /// </summary>
    /// <returns></returns>
    Task Prime();

    void Reset();
    Task ProcessSeriesAsync(IList<ParserInfo> parsedInfos, Library library, int totalToProcess, bool forceUpdate = false, GdsInfo gdsInfo=null);
}

/// <summary>
/// All code needed to Update a Series from a Scan action
/// </summary>
public class ProcessSeries : IProcessSeries
{
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<ProcessSeries> _logger;
    private readonly IEventHub _eventHub;
    private readonly IDirectoryService _directoryService;
    private readonly ICacheHelper _cacheHelper;
    private readonly IReadingItemService _readingItemService;
    private readonly IFileService _fileService;
    private readonly IMetadataService _metadataService;
    private readonly IMetadataServiceGds _metadataServiceGds;
    private readonly IWordCountAnalyzerService _wordCountAnalyzerService;
    private readonly IWordCountAnalyzerServiceGds _wordCountAnalyzerServiceGds;
    private readonly ICollectionTagService _collectionTagService;
    private readonly IReadingListService _readingListService;
    private readonly IExternalMetadataService _externalMetadataService;
    private readonly ITagManagerService _tagManagerService;
    private readonly IBookService _bookService;


    public ProcessSeries(IUnitOfWork unitOfWork, ILogger<ProcessSeries> logger, IEventHub eventHub,
        IDirectoryService directoryService, ICacheHelper cacheHelper, IReadingItemService readingItemService,
        IFileService fileService, IMetadataService metadataService, IMetadataServiceGds metadataServiceGds, IWordCountAnalyzerService wordCountAnalyzerService, IWordCountAnalyzerServiceGds wordCountAnalyzerServiceGds,
        ICollectionTagService collectionTagService, IReadingListService readingListService,
        IExternalMetadataService externalMetadataService, ITagManagerService tagManagerService, IBookService bookService)
    {
        _unitOfWork = unitOfWork;
        _logger = logger;
        _eventHub = eventHub;
        _directoryService = directoryService;
        _cacheHelper = cacheHelper;
        _readingItemService = readingItemService;
        _fileService = fileService;
        _metadataService = metadataService;
        _metadataServiceGds = metadataServiceGds;
        _wordCountAnalyzerService = wordCountAnalyzerService;
        _wordCountAnalyzerServiceGds = wordCountAnalyzerServiceGds;
        _collectionTagService = collectionTagService;
        _readingListService = readingListService;
        _externalMetadataService = externalMetadataService;
        _tagManagerService = tagManagerService;
        _bookService = bookService;
    }

    /// <summary>
    /// Invoke this before processing any series, just once to prime all the needed data during a scan
    /// </summary>
    public async Task Prime()
    {
        try
        {
            await _tagManagerService.Prime();
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Unable to prime tag manager. Scan cannot proceed. Report to Kavita dev");
        }
    }

    /// <summary>
    /// Frees up memory
    /// </summary>
    public void Reset()
    {
        _tagManagerService.Reset();
    }

    public async Task ProcessSeriesAsync(IList<ParserInfo> parsedInfos, Library library, int totalToProcess, bool forceUpdate = false, GdsInfo gdsInfo=null)
    {
        if (!parsedInfos.Any()) return;

        var seriesAdded = false;
        var scanWatch = Stopwatch.StartNew();
        var seriesName = parsedInfos[0].Series;
        await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
            MessageFactory.LibraryScanProgressEvent(library.Name, ProgressEventType.Updated, seriesName, totalToProcess));
        _logger.LogInformation("[ScannerService] Beginning series update on {SeriesName}, Forced: {ForceUpdate}", seriesName, forceUpdate);

        // Check if there is a Series
        var firstInfo = parsedInfos[0];
        Series? series;
        try
        {
            // There is an opportunity to allow duplicate series here. Like if One is in root/marvel/batman and another is root/dc/batman
            // by changing to a ToList() and if multiple, doing a firstInfo.FirstFolder/RootFolder type check
            series =
                await _unitOfWork.SeriesRepository.GetFullSeriesByAnyName(firstInfo.Series, firstInfo.LocalizedSeries,
                    library.Id, firstInfo.Format);
        }
        catch (Exception ex)
        {
            await ReportDuplicateSeriesLookup(library, firstInfo, ex);
            return;
        }

        if (series == null)
        {
            seriesAdded = true;
            series = new SeriesBuilder(firstInfo.Series)
                .WithLocalizedName(firstInfo.LocalizedSeries)
                .Build();
            _unitOfWork.SeriesRepository.Add(series);
        }

        if (series.LibraryId == 0) series.LibraryId = library.Id;

        if (library.Type == LibraryType.GDS && gdsInfo == null)
        {
            gdsInfo = GdsUtil.getGdsInfoByFile(firstInfo.FullFilePath);
            
        }
        if (gdsInfo != null && gdsInfo.action.ContainsKey("all_file_is_special") && gdsInfo.action["all_file_is_special"] == "true")
        {
            foreach(var info in parsedInfos)
            {
                info.IsSpecial = true;
                info.Volumes = "-100000"; // Parser.LooseLeafVolume;
            }
        }

        try
        {
            _logger.LogInformation("[ScannerService] Processing series {SeriesName}", series.OriginalName);

            // parsedInfos[0] is not the first volume or chapter. We need to find it using a ComicInfo check (as it uses firstParsedInfo for series sort)
            var firstParsedInfo = parsedInfos.FirstOrDefault(p => p.ComicInfo != null, firstInfo);

            await UpdateVolumes(series, parsedInfos, forceUpdate, gdsInfo);
            series.Pages = series.Volumes.Sum(v => v.Pages);

            series.NormalizedName = series.Name.ToNormalized();
            series.OriginalName ??= firstParsedInfo.Series;
            if (series.Format == MangaFormat.Unknown)
            {
                series.Format = firstParsedInfo.Format;
            }

            if (string.IsNullOrEmpty(series.SortName))
            {
                series.SortName = series.Name;
            }
            if (!series.SortNameLocked)
            {
                series.SortName = series.Name;
                if (!string.IsNullOrEmpty(firstParsedInfo.SeriesSort))
                {
                    series.SortName = firstParsedInfo.SeriesSort;
                }
            }
            series.SortName = series.SortName.Normalize(System.Text.NormalizationForm.FormKD);

            // parsedInfos[0] is not the first volume or chapter. We need to find it
            var localizedSeries = parsedInfos.Select(p => p.LocalizedSeries).FirstOrDefault(p => !string.IsNullOrEmpty(p));
            if (!series.LocalizedNameLocked && !string.IsNullOrEmpty(localizedSeries))
            {
                series.LocalizedName = localizedSeries;
                series.NormalizedLocalizedName = series.LocalizedName.ToNormalized();
            }

            await UpdateSeriesMetadata(series, library, gdsInfo);

            // Update series FolderPath here
            await UpdateSeriesFolderPath(parsedInfos, library, series);

            series.UpdateLastFolderScanned();

            if (_unitOfWork.HasChanges())
            {
                try
                {
                    await _unitOfWork.CommitAsync();
                }
                catch (DbUpdateConcurrencyException ex)
                {
                    foreach (var entry in ex.Entries)
                    {
                        if (entry.Entity is Series)
                        {
                            var proposedValues = entry.CurrentValues;
                            var databaseValues = await entry.GetDatabaseValuesAsync();

                            foreach (var property in proposedValues.Properties)
                            {
                                var proposedValue = proposedValues[property];
                                var databaseValue = databaseValues[property];

                                // TODO: decide which value should be written to database
                                _logger.LogDebug("Property conflict, proposed: {Proposed} vs db: {Database}", proposedValue, databaseValue);
                                // proposedValues[property] = <value to be saved>;
                            }

                            // Refresh original values to bypass next concurrency check
                            entry.OriginalValues.SetValues(databaseValues);
                        }
                    }


                    _logger.LogCritical(ex,
                        "[ScannerService] There was an issue writing to the database for series {SeriesName}",
                        series.Name);
                    await _eventHub.SendMessageAsync(MessageFactory.Error,
                        MessageFactory.ErrorEvent($"There was an issue writing to the DB for Series {series.OriginalName}",
                            ex.Message));
                    return;
                }
                catch (Exception ex)
                {
                    await _unitOfWork.RollbackAsync();
                    _logger.LogCritical(ex,
                        "[ScannerService] There was an issue writing to the database for series {SeriesName}",
                        series.Name);

                    await _eventHub.SendMessageAsync(MessageFactory.Error,
                        MessageFactory.ErrorEvent($"There was an issue writing to the DB for Series {series.OriginalName}",
                            ex.Message));
                    return;
                }


                // Process reading list after commit as we need to commit per list
                await _readingListService.CreateReadingListsFromSeries(library.Id, series.Id);

                if (seriesAdded)
                {
                    // See if any recommendations can link up to the series and pre-fetch external metadata for the series
                    _logger.LogInformation("Linking up External Recommendations new series (if applicable)");

                    // BackgroundJob.Enqueue(() =>
                    //     _externalMetadataService.GetNewSeriesData(series.Id, series.Library.Type));
                    await _externalMetadataService.GetNewSeriesData(series.Id, series.Library.Type);

                    await _eventHub.SendMessageAsync(MessageFactory.SeriesAdded,
                        MessageFactory.SeriesAddedEvent(series.Id, series.Name, series.LibraryId), false);
                }
                else
                {
                    await _unitOfWork.ExternalSeriesMetadataRepository.LinkRecommendationsToSeries(series);
                }

                _logger.LogInformation("[ScannerService] Finished series update on {SeriesName} in {Milliseconds} ms", seriesName, scanWatch.ElapsedMilliseconds);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[ScannerService] There was an exception updating series for {SeriesName}", series.Name);
            return;
        }

        var settings = await _unitOfWork.SettingsRepository.GetSettingsDtoAsync();

        if (series.Library.Type != LibraryType.GDS)
        {
            await _metadataService.GenerateCoversForSeries(series, settings.EncodeMediaAs, settings.CoverImageSize);
            await _wordCountAnalyzerService.ScanSeries(series.LibraryId, series.Id, forceUpdate);
        } else {
            await _metadataServiceGds.GenerateCoversForSeries(series, settings.EncodeMediaAs, settings.CoverImageSize, forceUpdate, gdsInfo);
            await _wordCountAnalyzerServiceGds.ScanSeries(series.LibraryId, series.Id, forceUpdate, gdsInfo);
        }
        
        // BackgroundJob.Enqueue(() => _wordCountAnalyzerService.ScanSeries(series.LibraryId, series.Id, forceUpdate));
        
    }


    private async Task ReportDuplicateSeriesLookup(Library library, ParserInfo firstInfo, Exception ex)
    {
        var seriesCollisions = await _unitOfWork.SeriesRepository.GetAllSeriesByAnyName(firstInfo.LocalizedSeries, string.Empty, library.Id, firstInfo.Format);

        seriesCollisions = seriesCollisions.Where(collision =>
            collision.Name != firstInfo.Series || collision.LocalizedName != firstInfo.LocalizedSeries).ToList();

        if (seriesCollisions.Count > 1)
        {
            var firstCollision = seriesCollisions[0];
            var secondCollision = seriesCollisions[1];

            var tableRows = $"<tr><td>Name: {firstCollision.Name}</td><td>Name: {secondCollision.Name}</td></tr>" +
                            $"<tr><td>Localized: {firstCollision.LocalizedName}</td><td>Localized: {secondCollision.LocalizedName}</td></tr>" +
                            $"<tr><td>Filename: {Parser.Parser.NormalizePath(firstCollision.FolderPath)}</td><td>Filename: {Parser.Parser.NormalizePath(secondCollision.FolderPath)}</td></tr>";

            var htmlTable = $"<table class='table table-striped'><thead><tr><th>Series 1</th><th>Series 2</th></tr></thead><tbody>{string.Join(string.Empty, tableRows)}</tbody></table>";

            _logger.LogError(ex, "Scanner found a Series {SeriesName} which matched another Series {LocalizedName} in a different folder parallel to Library {LibraryName} root folder. This is not allowed. Please correct",
                firstInfo.Series, firstInfo.LocalizedSeries, library.Name);

            await _eventHub.SendMessageAsync(MessageFactory.Error,
                MessageFactory.ErrorEvent($"Library {library.Name} Series collision on {firstInfo.Series}",
                    htmlTable));
        }
    }


    private async Task UpdateSeriesFolderPath(IEnumerable<ParserInfo> parsedInfos, Library library, Series series)
    {
        var libraryFolders = library.Folders.Select(l => Parser.Parser.NormalizePath(l.Path)).ToList();
        var seriesFiles = parsedInfos.Select(f => Parser.Parser.NormalizePath(f.FullFilePath)).ToList();
        var seriesDirs = _directoryService.FindHighestDirectoriesFromFiles(libraryFolders, seriesFiles);
        if (seriesDirs.Keys.Count == 0)
        {
            _logger.LogCritical(
                "Scan Series has files spread outside a main series folder. This has negative performance effects. Please ensure all series are under a single folder from library");
            await _eventHub.SendMessageAsync(MessageFactory.Info,
                MessageFactory.InfoEvent($"{series.Name} has files spread outside a single series folder",
                    "This has negative performance effects. Please ensure all series are under a single folder from library"));
        }
        else
        {
            // Don't save FolderPath if it's a library Folder
            if (!library.Folders.Select(f => f.Path).Contains(seriesDirs.Keys.First()))
            {
                // BUG: FolderPath can be a level higher than it needs to be. I'm not sure why it's like this, but I thought it should be one level lower.
                // I think it's like this because higher level is checked or not checked. But i think we can do both
                series.FolderPath = Parser.Parser.NormalizePath(seriesDirs.Keys.First());
                _logger.LogDebug("Updating {Series} FolderPath to {FolderPath}", series.Name, series.FolderPath);
            }
        }

        var lowestFolder = _directoryService.FindLowestDirectoriesFromFiles(libraryFolders, seriesFiles);
        if (!string.IsNullOrEmpty(lowestFolder))
        {
            series.LowestFolderPath = lowestFolder;
            _logger.LogDebug("Updating {Series} LowestFolderPath to {FolderPath}", series.Name, series.LowestFolderPath);
        }
        if (library.Type == LibraryType.GDS)
        {
            series.FolderPath = series.LowestFolderPath;
        }
    }


    private async Task UpdateSeriesMetadata(Series series, Library library, GdsInfo gdsInfo)
    {
        series.Metadata ??= new SeriesMetadataBuilder().Build();
        var firstChapter = SeriesService.GetFirstChapterForMetadata(series);

        var firstFile = firstChapter?.Files.FirstOrDefault();
        if (firstFile == null) return;
        // 원본은 pdf 메타 처리 없음.
        bool isPdf = Parser.Parser.IsPdf(firstFile.FilePath);
        if (gdsInfo == null && isPdf) return;

        var chapters = series.Volumes.SelectMany(volume => volume.Chapters).ToList();

        // Update Metadata based on Chapter metadata
        if (!series.Metadata.ReleaseYearLocked)
        {
            try
            {
                if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Year"]))
                {
                    series.Metadata.ReleaseYear = Int32.Parse(gdsInfo.meta["Year"]);
                }
                else
                {
                    if (!isPdf) series.Metadata.ReleaseYear = chapters.MinimumReleaseYear();
                }
            }
            catch(Exception ex) {}
        }

        // Set the AgeRating as highest in all the comicInfos
        if (!series.Metadata.AgeRatingLocked)
        {
            try
            {
                if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Age Rating"]))
                {
                    series.Metadata.AgeRating = (AgeRating)Int32.Parse(gdsInfo.meta["Age Rating"]);
                }
                else
                {
                    if (!isPdf) series.Metadata.AgeRating = chapters.Max(chapter => chapter.AgeRating);
                }
            }
            catch (Exception ex) { }

        }
        try
        {
            if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Publication Status"]))
            {
                series.Metadata.PublicationStatus = (PublicationStatus)Int32.Parse(gdsInfo.meta["Publication Status"]);
            }
            else
            {
                if (!isPdf) DeterminePublicationStatus(series, chapters);
            }
        }
        catch (Exception ex) { }

        if (!series.Metadata.SummaryLocked)
        {
            if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null)
            {
                series.Metadata.Summary = "";
                if (!string.IsNullOrEmpty(gdsInfo.meta["Summary"])) series.Metadata.Summary = gdsInfo.meta["Summary"];
            } else
            {
                if (!isPdf && !string.IsNullOrEmpty(firstChapter?.Summary) && !series.Metadata.SummaryLocked)
                {
                    series.Metadata.Summary = firstChapter.Summary;
                }
            }
        }

        if (!series.Metadata.LanguageLocked)
        {
            if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null)
            { 
                series.Metadata.Language = gdsInfo.meta["Language"];
                if (!string.IsNullOrEmpty(gdsInfo.meta["Language"])) series.Metadata.Language = gdsInfo.meta["Language"];
            }
            else
            {
                if (!isPdf && !string.IsNullOrEmpty(firstChapter?.Language) && !series.Metadata.LanguageLocked)
                {
                    series.Metadata.Language = firstChapter.Language;
                }
            }
        }


        if (library.ManageCollections && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Collections"]))
        {
            // Get the default admin to associate these tags to
            var defaultAdmin = await _unitOfWork.UserRepository.GetDefaultAdminUser(AppUserIncludes.Collections);
            if (defaultAdmin == null) return;

            _logger.LogDebug("Collection tag(s) found for {SeriesName}, updating collections", series.Name);
            foreach (var collection in gdsInfo.meta["Collections"].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            {
                var t = await _tagManagerService.GetCollectionTag(collection, defaultAdmin);
                if (t.Item1 == null) continue;
                var tag = t.Item1;
                if (tag.Items.Any(s => s.MatchesSeriesByName(series.NormalizedName, series.NormalizedLocalizedName)))
                {
                    continue;
                }
                tag.Items.Add(series);
                await _unitOfWork.CollectionTagRepository.UpdateCollectionAgeRating(tag);
            }
        }
        else
        {
            if (!isPdf && !string.IsNullOrEmpty(firstChapter?.SeriesGroup) && library.ManageCollections)
            {
                // Get the default admin to associate these tags to
                var defaultAdmin = await _unitOfWork.UserRepository.GetDefaultAdminUser(AppUserIncludes.Collections);
                if (defaultAdmin == null) return;

                _logger.LogDebug("Collection tag(s) found for {SeriesName}, updating collections", series.Name);
                foreach (var collection in firstChapter.SeriesGroup.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
                {
                    var t = await _tagManagerService.GetCollectionTag(collection, defaultAdmin);
                    if (t.Item1 == null) continue;

                    var tag = t.Item1;

                    // Check if the Series is already on the tag
                    if (tag.Items.Any(s => s.MatchesSeriesByName(series.NormalizedName, series.NormalizedLocalizedName)))
                    {
                        continue;
                    }

                    tag.Items.Add(series);
                    await _unitOfWork.CollectionTagRepository.UpdateCollectionAgeRating(tag);
                }
            }
        }

        if (!series.Metadata.GenresLocked)
        {
            if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null)
            {
                series.Metadata.Genres.Clear();
                if (!string.IsNullOrEmpty(gdsInfo.meta["Genres"]))
                {
                    var genres = TagHelper.GetTagValues(gdsInfo.meta["Genres"]);
                    GenreHelper.KeepOnlySameGenreBetweenLists(series.Metadata.Genres,
                        genres.Select(g => new GenreBuilder(g).Build()).ToList());
                    foreach (var genre in genres)
                    {
                        var g = await _tagManagerService.GetGenre(genre);
                        if (g == null) continue;
                        series.Metadata.Genres.Add(g);
                    }
                }
            } else
            {
                if (!isPdf) { 
                    var genres = chapters.SelectMany(c => c.Genres).ToList();
                    GenreHelper.KeepOnlySameGenreBetweenLists(series.Metadata.Genres.ToList(), genres, genre =>
                        {
                            series.Metadata.Genres.Remove(genre);
                        });
                }
            }
        }


        #region People
        series.Metadata.People.Clear();
        // Handle People
        if (!isPdf)
        {
            foreach (var chapter in chapters)
            {
                if (!series.Metadata.WriterLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Writer))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.CoverArtistLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.CoverArtist))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.PublisherLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Publisher))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.CharacterLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Character))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.ColoristLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Colorist))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.EditorLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Editor))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.InkerLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Inker))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.ImprintLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Imprint))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.TeamLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Team))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.LocationLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Location))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.LettererLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Letterer))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.PencillerLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Penciller))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.TranslatorLocked)
                {
                    foreach (var person in chapter.People.Where(p => p.Role == PersonRole.Translator))
                    {
                        PersonHelper.AddPersonIfNotExists(series.Metadata.People, person);
                    }
                }

                if (!series.Metadata.TagsLocked)
                {
                    foreach (var tag in chapter.Tags)
                    {
                        TagHelper.AddTagIfNotExists(series.Metadata.Tags, tag);
                    }
                }

                if (!series.Metadata.GenresLocked)
                {
                    foreach (var genre in chapter.Genres)
                    {
                        GenreHelper.AddGenreIfNotExists(series.Metadata.Genres, genre);
                    }
                }
            }
            // NOTE: The issue here is that people is just from chapter, but series metadata might already have some people on it
            // I might be able to filter out people that are in locked fields?
            var people = chapters.SelectMany(c => c.People).ToList();
            PersonHelper.KeepOnlySamePeopleBetweenLists(series.Metadata.People.ToList(),
                people, person =>
                {
                    switch (person.Role)
                    {
                        case PersonRole.Writer:
                            if (!series.Metadata.WriterLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Penciller:
                            if (!series.Metadata.PencillerLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Inker:
                            if (!series.Metadata.InkerLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Imprint:
                            if (!series.Metadata.ImprintLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Colorist:
                            if (!series.Metadata.ColoristLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Letterer:
                            if (!series.Metadata.LettererLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.CoverArtist:
                            if (!series.Metadata.CoverArtistLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Editor:
                            if (!series.Metadata.EditorLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Publisher:
                            if (!series.Metadata.PublisherLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Character:
                            if (!series.Metadata.CharacterLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Translator:
                            if (!series.Metadata.TranslatorLocked) series.Metadata.People.Remove(person);
                            break;
                        case PersonRole.Other:
                        default:
                            series.Metadata.People.Remove(person);
                            break;
                    }
                });

            #endregion
        }

        if (!series.Metadata.WriterLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Writers"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Writers"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Writer);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Writer);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.PencillerLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Penciller"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Penciller"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Penciller);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Penciller);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.PublisherLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Publisher"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Publisher"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Publisher);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Publisher);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.CharacterLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Character"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Character"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Character);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Character);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.ColoristLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Colorist"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Colorist"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Colorist);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Colorist);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.EditorLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Editor"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Editor"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Editor);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Editor);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.InkerLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Inker"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Inker"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Inker);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Inker);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.ImprintLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Imprint"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Imprint"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Imprint);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Imprint);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.TeamLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Team"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Team"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Team);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Team);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.LocationLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Location"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Location"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Location);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Location);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.LettererLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Letterer"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Letterer"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Letterer);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Letterer);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.TranslatorLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person Translator"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person Translator"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.Translator);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.Translator);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.CoverArtistLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null && !string.IsNullOrEmpty(gdsInfo.meta["Person CoverArtist"]))
        {
            var people = TagHelper.GetTagValues(gdsInfo.meta["Person CoverArtist"]);
            PersonHelper.RemovePeople(series.Metadata.People, people, PersonRole.CoverArtist);
            foreach (var person in people)
            {
                var p = await _tagManagerService.GetPerson(person, PersonRole.CoverArtist);
                if (p == null) continue;
                series.Metadata.People.Add(p);
            }
        }

        if (!series.Metadata.TagsLocked && library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null)
        {
            series.Metadata.Tags.Clear();
            if (!string.IsNullOrEmpty(gdsInfo.meta["Tags"]))
            {
                var tags = TagHelper.GetTagValues(gdsInfo.meta["Tags"]);
                TagHelper.KeepOnlySameTagBetweenLists(series.Metadata.Tags, tags.Select(t => new TagBuilder(t).Build()).ToList());
                foreach (var tag in tags)
                {
                    var t = await _tagManagerService.GetTag(tag);
                    if (t == null) continue;
                    series.Metadata.Tags.Add(t);
                }
            }
        }


        if (library.Type == LibraryType.GDS && gdsInfo != null && gdsInfo.meta != null)
        { 
            series.Metadata.WebLinks = "";
            if (!string.IsNullOrEmpty(gdsInfo.meta["Web Links"])) series.Metadata.WebLinks = string.Join(",", gdsInfo.meta["Web Links"].Split(",", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            );
        }
    }

    private void DeterminePublicationStatus(Series series, List<Chapter> chapters)
    {
        try
        {
            // Count (aka expected total number of chapters or volumes from metadata) across all chapters
            series.Metadata.TotalCount = chapters.Max(chapter => chapter.TotalCount);
            // The actual number of count's defined across all chapter's metadata
            series.Metadata.MaxCount = chapters.Max(chapter => chapter.Count);

            var nonSpecialVolumes = series.Volumes.Where(v => v.MaxNumber.IsNot(Parser.Parser.SpecialVolumeNumber));

            var maxVolume = (int) (nonSpecialVolumes.Any() ? nonSpecialVolumes.Max(v => v.MaxNumber) : 0);
            var maxChapter = (int) chapters.Max(c => c.MaxNumber);

            // Single books usually don't have a number in their Range (filename)
            if (series.Format == MangaFormat.Epub || series.Format == MangaFormat.Pdf || series.Format == MangaFormat.Text && chapters.Count == 1)
            {
                series.Metadata.MaxCount = 1;
            }
            else if (series.Metadata.TotalCount <= 1 && chapters.Count == 1 && chapters[0].IsSpecial)
            {
                // If a series has a TotalCount of 1 (or no total count) and there is only a Special, mark it as Complete
                series.Metadata.MaxCount = series.Metadata.TotalCount;
            }
            else if ((maxChapter == Parser.Parser.DefaultChapterNumber || maxChapter > series.Metadata.TotalCount) &&
                     maxVolume <= series.Metadata.TotalCount)
            {
                series.Metadata.MaxCount = maxVolume;
            }
            else if (maxVolume == series.Metadata.TotalCount)
            {
                series.Metadata.MaxCount = maxVolume;
            }
            else
            {
                series.Metadata.MaxCount = maxChapter;
            }

            if (!series.Metadata.PublicationStatusLocked)
            {
                series.Metadata.PublicationStatus = PublicationStatus.OnGoing;
                if (series.Metadata.MaxCount == series.Metadata.TotalCount && series.Metadata.TotalCount > 0)
                {
                    series.Metadata.PublicationStatus = PublicationStatus.Completed;
                }
                else if (series.Metadata.TotalCount > 0 && series.Metadata.MaxCount > 0)
                {
                    series.Metadata.PublicationStatus = PublicationStatus.Ended;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "There was an issue determining Publication Status");
            series.Metadata.PublicationStatus = PublicationStatus.OnGoing;
        }
    }

    private async Task UpdateVolumes(Series series, IList<ParserInfo> parsedInfos, bool forceUpdate = false, GdsInfo gdsInfo=null)
    {
        // Add new volumes and update chapters per volume
        var distinctVolumes = parsedInfos.DistinctVolumes();
        _logger.LogDebug("[ScannerService] Updating {DistinctVolumes} volumes on {SeriesName}", distinctVolumes.Count, series.Name);

        
        foreach (var volumeNumber in distinctVolumes)
        {
            Volume? volume;
            try
            {
                // With the Name change to be formatted, Name no longer working because Name returns "1" and volumeNumber is "1.0", so we use LookupName as the original
                volume = series.Volumes.SingleOrDefault(s => s.LookupName == volumeNumber);
            }
            catch (Exception ex)
            {
                // TODO: Push this to UI in some way
                if (!ex.Message.Equals("Sequence contains more than one matching element")) throw;
                _logger.LogCritical(ex, "[ScannerService] Kavita found corrupted volume entries on {SeriesName}. Please delete the series from Kavita via UI and rescan", series.Name);
                throw new KavitaException(
                    $"Kavita found corrupted volume entries on {series.Name}. Please delete the series from Kavita via UI and rescan");
            }
            if (volume == null)
            {
                volume = new VolumeBuilder(volumeNumber)
                    .WithSeriesId(series.Id)
                    .Build();
                series.Volumes.Add(volume);
            }

            volume.LookupName = volumeNumber;
            volume.Name = volume.GetNumberTitle();

            _logger.LogDebug("[ScannerService] Parsing {SeriesName} - Volume {VolumeNumber}", series.Name, volume.Name);
            var infos = parsedInfos.Where(p => p.Volumes == volumeNumber).ToArray();
            UpdateChapters(series, volume, infos, forceUpdate, gdsInfo);
            volume.Pages = volume.Chapters.Sum(c => c.Pages);

            // Update all the metadata on the Chapters
            foreach (var chapter in volume.Chapters)
            {
                var firstFile = chapter.Files.MinBy(x => x.Chapter);
                if (firstFile == null || _cacheHelper.IsFileUnmodifiedSinceCreationOrLastScan(chapter, forceUpdate, firstFile)) continue;
                try
                {
                    var firstChapterInfo = infos.SingleOrDefault(i => i.FullFilePath.Equals(firstFile.FilePath));
                    await UpdateChapterFromComicInfo(chapter, firstChapterInfo?.ComicInfo, forceUpdate);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "There was some issue when updating chapter's metadata");
                }
            }
        }

        // Remove existing volumes that aren't in parsedInfos
        var nonDeletedVolumes = series.Volumes
            .Where(v => parsedInfos.Select(p => p.Volumes).Contains(v.LookupName))
            .ToList();
        if (series.Volumes.Count != nonDeletedVolumes.Count)
        {
            _logger.LogDebug("[ScannerService] Removed {Count} volumes from {SeriesName} where parsed infos were not mapping with volume name",
                (series.Volumes.Count - nonDeletedVolumes.Count), series.Name);
            var deletedVolumes = series.Volumes.Except(nonDeletedVolumes);
            foreach (var volume in deletedVolumes)
            {
                var file = volume.Chapters.FirstOrDefault()?.Files?.FirstOrDefault()?.FilePath ?? string.Empty;
                if (!string.IsNullOrEmpty(file) && _directoryService.FileSystem.File.Exists(file))
                {
                    // This can happen when file is renamed and volume is removed
                    _logger.LogInformation(
                        "[ScannerService] Volume cleanup code was trying to remove a volume with a file still existing on disk (usually volume marker removed) File: {File}",
                        file);
                }

                _logger.LogDebug("[ScannerService] Removed {SeriesName} - Volume {Volume}: {File}", series.Name, volume.Name, file);
            }

            series.Volumes = nonDeletedVolumes;
        }
    }

    private void UpdateChapters(Series series, Volume volume, IList<ParserInfo> parsedInfos, bool forceUpdate = false, GdsInfo gdsInfo=null)
    {
        
        // Add new chapters
        foreach (var info in parsedInfos)
        {
            // Specials go into their own chapters with Range being their filename and IsSpecial = True. Non-Specials with Vol and Chap as 0
            // also are treated like specials for UI grouping.
            Chapter? chapter;
            try
            {
                chapter = volume.Chapters.GetChapterByRange(info);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{FileName} mapped as '{Series} - Vol {Volume} Ch {Chapter}' is a duplicate, skipping", info.FullFilePath, info.Series, info.Volumes, info.Chapters);
                continue;
            }
            
            if (chapter == null)
            {
                _logger.LogDebug(
                    "[ScannerService] Adding new chapter, {Series} - Vol {Volume} Ch {Chapter}", info.Series, info.Volumes, info.Chapters);
                chapter = ChapterBuilder.FromParserInfo(info).Build();
                volume.Chapters.Add(chapter);
                series.UpdateLastChapterAdded();
            }
            else
            {
                chapter.UpdateFrom(info);
            }

            if (chapter == null)
            {
                continue;
            }
            // Add files
            AddOrUpdateFileForChapter(chapter, info, forceUpdate, gdsInfo);

            // TODO: Investigate using the ChapterBuilder here
            chapter.Number = Parser.Parser.MinNumberFromRange(info.Chapters).ToString(CultureInfo.InvariantCulture);
            chapter.MinNumber = Parser.Parser.MinNumberFromRange(info.Chapters);
            chapter.MaxNumber = Parser.Parser.MaxNumberFromRange(info.Chapters);
            if (!chapter.SortOrderLocked)
            {
                chapter.SortOrder = info.IssueOrder;
            }
            chapter.Range = chapter.GetNumberTitle();
            if (float.TryParse(chapter.Title, out var _))
            {
                // If we have float based chapters, first scan can have the chapter formatted as Chapter 0.2 - .2 as the title is wrong.
                chapter.Title = chapter.GetNumberTitle();
            }

        }


        // Remove chapters that aren't in parsedInfos or have no files linked
        var existingChapters = volume.Chapters.ToList();
        foreach (var existingChapter in existingChapters)
        {
            if (existingChapter.Files.Count == 0 || !parsedInfos.HasInfo(existingChapter))
            {
                _logger.LogDebug("[ScannerService] Removed chapter {Chapter} for Volume {VolumeNumber} on {SeriesName}",
                    existingChapter.Range, volume.Name, parsedInfos[0].Series);
                volume.Chapters.Remove(existingChapter);
            }
            else
            {
                // Ensure we remove any files that no longer exist AND order
                existingChapter.Files = existingChapter.Files
                    .Where(f => parsedInfos.Any(p => Parser.Parser.NormalizePath(p.FullFilePath) == Parser.Parser.NormalizePath(f.FilePath)))
                    .OrderByNatural(f => f.FilePath)
                    .ToList();
                existingChapter.Pages = existingChapter.Files.Sum(f => f.Pages);
            }
        }
    }

    private void AddOrUpdateFileForChapter(Chapter chapter, ParserInfo info, bool forceUpdate = false, GdsInfo gdsInfo=null)
    {
        chapter.Files ??= new List<MangaFile>();
        var existingFile = chapter.Files.SingleOrDefault(f => f.FilePath == info.FullFilePath);
        var fileInfo = _directoryService.FileSystem.FileInfo.New(info.FullFilePath);
        if (existingFile != null)
        {
            existingFile.Format = info.Format;
            if (!forceUpdate && !_fileService.HasFileBeenModifiedSince(existingFile.FilePath, existingFile.LastModified) && existingFile.Pages != 0) return;
            bool flagPage = false;
            if (gdsInfo != null) // && gdsInfo.files.Contains()
            {
                var key = Path.GetFileName(existingFile.FilePath);
                var gdsFile = GdsUtil.GetGdsFile(gdsInfo, key);
                if (gdsFile != null)
                {
                    if (Parser.Parser.IsText(key))
                    {
                        int pages = gdsFile.page / _bookService.GetTextLinesPerPage();
                        if (gdsFile.page % _bookService.GetTextLinesPerPage() > 0) pages += 1;
                        existingFile.Pages = pages;
                    } else
                    {
                        existingFile.Pages = gdsFile.page;
                    }
                    flagPage = true;
                }
            }
            if (flagPage == false)
                existingFile.Pages = _readingItemService.GetNumberOfPages(info.FullFilePath, info.Format);
            existingFile.Extension = fileInfo.Extension.ToLowerInvariant();
            existingFile.FileName = Parser.Parser.RemoveExtensionIfSupported(existingFile.FilePath);
            existingFile.FilePath = Parser.Parser.NormalizePath(existingFile.FilePath);
            existingFile.Bytes = fileInfo.Length;
            // We skip updating DB here with last modified time so that metadata refresh can do it
        }
        else
        {
            var pages = 1;
            bool flagPage = false;
            if (gdsInfo != null) // && gdsInfo.files.Contains()
            {
                var key = Path.GetFileName(info.FullFilePath);
                var gdsFile = GdsUtil.GetGdsFile(gdsInfo, key);
                if (gdsFile != null)
                {
                    pages = gdsFile.page;
                    flagPage = true;
                }
            }
            if (flagPage == false)
                pages = _readingItemService.GetNumberOfPages(info.FullFilePath, info.Format);
            var file = new MangaFileBuilder(info.FullFilePath, info.Format, pages)
                .WithExtension(fileInfo.Extension)
                .WithBytes(fileInfo.Length)
                .Build();
            chapter.Files.Add(file);
        }
    }

    private async Task UpdateChapterFromComicInfo(Chapter chapter, ComicInfo? comicInfo, bool forceUpdate = false)
    {
        if (comicInfo == null) return;
        var firstFile = chapter.Files.MinBy(x => x.Chapter);
        if (firstFile == null ||
            _cacheHelper.IsFileUnmodifiedSinceCreationOrLastScan(chapter, forceUpdate, firstFile)) return;

        _logger.LogTrace("[ScannerService] Read ComicInfo for {File}", firstFile.FilePath);

        chapter.AgeRating = ComicInfo.ConvertAgeRatingToEnum(comicInfo.AgeRating);

        if (!string.IsNullOrEmpty(comicInfo.Title))
        {
            chapter.TitleName = comicInfo.Title.Trim();
        }

        if (!string.IsNullOrEmpty(comicInfo.Summary))
        {
            chapter.Summary = comicInfo.Summary;
        }

        if (!string.IsNullOrEmpty(comicInfo.LanguageISO))
        {
            chapter.Language = comicInfo.LanguageISO;
        }

        if (!string.IsNullOrEmpty(comicInfo.SeriesGroup))
        {
            chapter.SeriesGroup = comicInfo.SeriesGroup;
        }

        if (!string.IsNullOrEmpty(comicInfo.StoryArc))
        {
            chapter.StoryArc = comicInfo.StoryArc;
        }

        if (!string.IsNullOrEmpty(comicInfo.AlternateSeries))
        {
            chapter.AlternateSeries = comicInfo.AlternateSeries;
        }

        if (!string.IsNullOrEmpty(comicInfo.AlternateNumber))
        {
            chapter.AlternateNumber = comicInfo.AlternateNumber;
        }

        if (!string.IsNullOrEmpty(comicInfo.StoryArcNumber))
        {
            chapter.StoryArcNumber = comicInfo.StoryArcNumber;
        }

        if (comicInfo.AlternateCount > 0)
        {
            chapter.AlternateCount = comicInfo.AlternateCount;
        }

        if (!string.IsNullOrEmpty(comicInfo.Web))
        {
            chapter.WebLinks = string.Join(",", comicInfo.Web
                .Split(",", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            );

            // For each weblink, try to parse out some MetadataIds and store in the Chapter directly for matching (CBL)
        }

        if (!string.IsNullOrEmpty(comicInfo.Isbn))
        {
            chapter.ISBN = comicInfo.Isbn;
        }

        if (comicInfo.Count > 0)
        {
            chapter.TotalCount = comicInfo.Count;
        }

        // This needs to check against both Number and Volume to calculate Count
        chapter.Count = comicInfo.CalculatedCount();


        if (comicInfo.Year > 0)
        {
            var day = Math.Max(comicInfo.Day, 1);
            var month = Math.Max(comicInfo.Month, 1);
            chapter.ReleaseDate = new DateTime(comicInfo.Year, month, day);
        }

        var people = TagHelper.GetTagValues(comicInfo.Colorist);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Colorist);
        await UpdatePeople(chapter, people, PersonRole.Colorist);

        people = TagHelper.GetTagValues(comicInfo.Characters);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Character);
        await UpdatePeople(chapter, people, PersonRole.Character);


        people = TagHelper.GetTagValues(comicInfo.Translator);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Translator);
        await UpdatePeople(chapter, people, PersonRole.Translator);


        people = TagHelper.GetTagValues(comicInfo.Writer);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Writer);
        await UpdatePeople(chapter, people, PersonRole.Writer);

        people = TagHelper.GetTagValues(comicInfo.Editor);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Editor);
        await UpdatePeople(chapter, people, PersonRole.Editor);

        people = TagHelper.GetTagValues(comicInfo.Inker);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Inker);
        await UpdatePeople(chapter, people, PersonRole.Inker);

        people = TagHelper.GetTagValues(comicInfo.Letterer);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Letterer);
        await UpdatePeople(chapter, people, PersonRole.Letterer);

        people = TagHelper.GetTagValues(comicInfo.Penciller);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Penciller);
        await UpdatePeople(chapter, people, PersonRole.Penciller);

        people = TagHelper.GetTagValues(comicInfo.CoverArtist);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.CoverArtist);
        await UpdatePeople(chapter, people, PersonRole.CoverArtist);

        people = TagHelper.GetTagValues(comicInfo.Publisher);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Publisher);
        await UpdatePeople(chapter, people, PersonRole.Publisher);

        people = TagHelper.GetTagValues(comicInfo.Imprint);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Imprint);
        await UpdatePeople(chapter, people, PersonRole.Imprint);

        people = TagHelper.GetTagValues(comicInfo.Teams);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Team);
        await UpdatePeople(chapter, people, PersonRole.Team);

        people = TagHelper.GetTagValues(comicInfo.Locations);
        PersonHelper.RemovePeople(chapter.People, people, PersonRole.Location);
        await UpdatePeople(chapter, people, PersonRole.Location);

        var genres = TagHelper.GetTagValues(comicInfo.Genre);
        GenreHelper.KeepOnlySameGenreBetweenLists(chapter.Genres,
            genres.Select(g => new GenreBuilder(g).Build()).ToList());
        foreach (var genre in genres)
        {
            var g = await _tagManagerService.GetGenre(genre);
            if (g == null) continue;
            chapter.Genres.Add(g);
        }

        var tags = TagHelper.GetTagValues(comicInfo.Tags);
        TagHelper.KeepOnlySameTagBetweenLists(chapter.Tags, tags.Select(t => new TagBuilder(t).Build()).ToList());
        foreach (var tag in tags)
        {
            var t = await _tagManagerService.GetTag(tag);
            if (t == null) continue;
            chapter.Tags.Add(t);
        }
    }

    private async Task UpdatePeople(Chapter chapter, IList<string> people, PersonRole role)
    {
        foreach (var person in people)
        {
            var p = await _tagManagerService.GetPerson(person, role);
            if (p == null) continue;
            chapter.People.Add(p);
        }
    }
}
