using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using API.Comparators;
using API.Data;
using API.Entities;
using API.Entities.Enums;
using API.Extensions;
using API.Helpers;
using API.SignalR;
using Hangfire;
using Microsoft.Extensions.Logging;
using API.Services.Tasks;

namespace API.Services;
#nullable enable

public interface IMetadataServiceGds
{
    /// <summary>
    /// Recalculates cover images for all entities in a library.
    /// </summary>
    /// <param name="libraryId"></param>
    /// <param name="forceUpdate"></param>
    [DisableConcurrentExecution(timeoutInSeconds: 60 * 60 * 60)]
    [AutomaticRetry(Attempts = 3, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
    Task GenerateCoversForLibrary(int libraryId, bool forceUpdate = false);
    /// <summary>
    /// Performs a forced refresh of cover images just for a series and it's nested entities
    /// </summary>
    /// <param name="libraryId"></param>
    /// <param name="seriesId"></param>
    /// <param name="forceUpdate">Overrides any cache logic and forces execution</param>

    Task GenerateCoversForSeries(int libraryId, int seriesId, bool forceUpdate = true, GdsInfo gdsInfo=null);
    Task GenerateCoversForSeries(Series series, EncodeFormat encodeFormat, CoverImageSize coverImageSize, bool forceUpdate = false, GdsInfo gdsInfo=null);
    Task RemoveAbandonedMetadataKeys();
}



public class MetadataServiceGds : IMetadataServiceGds
{
    public const string Name = "MetadataServiceGDS";
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<MetadataServiceGds> _logger;
    private readonly IEventHub _eventHub;
    private readonly ICacheHelper _cacheHelper;
    private readonly IReadingItemService _readingItemService;
    private readonly IDirectoryService _directoryService;
    private readonly IList<SignalRMessage> _updateEvents = new List<SignalRMessage>();
    public MetadataServiceGds(IUnitOfWork unitOfWork, ILogger<MetadataServiceGds> logger,
        IEventHub eventHub, ICacheHelper cacheHelper,
        IReadingItemService readingItemService, IDirectoryService directoryService)
    {
        _unitOfWork = unitOfWork;
        _logger = logger;
        _eventHub = eventHub;
        _cacheHelper = cacheHelper;
        _readingItemService = readingItemService;
        _directoryService = directoryService;
    }

    /// <summary>
    /// Updates the metadata for a Chapter
    /// </summary>
    /// <param name="chapter"></param>
    /// <param name="forceUpdate">Force updating cover image even if underlying file has not been modified or chapter already has a cover image</param>
    /// <param name="encodeFormat">Convert image to Encoding Format when extracting the cover</param>
    private Task<bool> UpdateChapterCoverImage(Chapter chapter, bool forceUpdate, EncodeFormat encodeFormat, CoverImageSize coverImageSize, GdsInfo gdsInfo)
    {
        var firstFile = chapter.Files.MinBy(x => x.Chapter);
        if (firstFile == null) return Task.FromResult(false);

        if (!_cacheHelper.ShouldUpdateCoverImage(_directoryService.FileSystem.Path.Join(_directoryService.CoverImageDirectory, chapter.CoverImage),
                firstFile, chapter.Created, forceUpdate, chapter.CoverImageLocked))
            return Task.FromResult(false);

        _logger.LogDebug("[MetadataServiceGDS] Generating cover image for {File}", firstFile.FilePath);
                
        bool flagGdsInfo = false;
        bool flagFirst = false;
        if (gdsInfo != null) // && gdsInfo.files.Contains()
        {
            var key = Path.GetFileName(firstFile.FilePath);
            var gdsFile = GdsUtil.GetGdsFile(gdsInfo, key);
            if (gdsFile != null)
            {
                //existingFile.Pages = gdsFile.page;
                //flagGdsInfo = true;
                if (gdsFile.cover == "FIRST")
                {
                    flagFirst = true;
                } else
                {
                    var filePath = Path.Join(_directoryService.CoverImageDirectory, ImageService.GetChapterFormat(chapter.Id, chapter.VolumeId)) + ".png";
                    flagGdsInfo = GdsUtil.saveCover(filePath, gdsFile.cover);
                    _logger.LogDebug("[MetadataServiceGDS] info로 썸네일 생성. {key} {flagGdsInfo}", key, flagGdsInfo);
                    if (flagGdsInfo)
                    {
                        chapter.CoverImage = ImageService.GetChapterFormat(chapter.Id, chapter.VolumeId) + ".png";
                    }
                }
            }
        }
        if (flagFirst == false)
        {
            if (flagGdsInfo == false)
            {
                // SOJU6JAN READ POINT - 커버
                _logger.LogWarning("[MetadataServiceGDS] 파일 오픈 시도 - 커버 {File}", firstFile.FilePath);
                chapter.CoverImage = _readingItemService.GetCoverImage(firstFile.FilePath,
                ImageService.GetChapterFormat(chapter.Id, chapter.VolumeId), firstFile.Format, encodeFormat, coverImageSize);
            }
            _unitOfWork.ChapterRepository.Update(chapter);

            _updateEvents.Add(MessageFactory.CoverUpdateEvent(chapter.Id, MessageFactoryEntityTypes.Chapter));
        }
        return Task.FromResult(true);
    }

    private void UpdateChapterLastModified(Chapter chapter, bool forceUpdate)
    {
        var firstFile = chapter.Files.MinBy(x => x.Chapter);
        if (firstFile == null || _cacheHelper.IsFileUnmodifiedSinceCreationOrLastScan(chapter, forceUpdate, firstFile)) return;

        firstFile.UpdateLastModified();
    }

    /// <summary>
    /// Updates the cover image for a Volume
    /// </summary>
    /// <param name="volume"></param>
    /// <param name="forceUpdate">Force updating cover image even if underlying file has not been modified or chapter already has a cover image</param>
    private Task<bool> UpdateVolumeCoverImage(API.Entities.Volume? volume, bool forceUpdate)
    {
        // We need to check if Volume coverImage matches first chapters if forceUpdate is false
        if (volume == null || !_cacheHelper.ShouldUpdateCoverImage(
                _directoryService.FileSystem.Path.Join(_directoryService.CoverImageDirectory, volume.CoverImage),
                null, volume.Created, forceUpdate)) return Task.FromResult(false);


        // For cover selection, chapters need to try for issue 1 first, then fallback to first sort order
        volume.Chapters ??= new List<Chapter>();

        var firstChapter = volume.Chapters.FirstOrDefault(x => x.MinNumber.Is(1f));
        if (firstChapter == null)
        {
            firstChapter = volume.Chapters.MinBy(x => x.SortOrder, ChapterSortComparerDefaultFirst.Default);
            if (firstChapter == null) return Task.FromResult(false);
        }

        volume.CoverImage = firstChapter.CoverImage;
        _updateEvents.Add(MessageFactory.CoverUpdateEvent(volume.Id, MessageFactoryEntityTypes.Volume));

        return Task.FromResult(true);
    }

    /// <summary>
    /// Updates cover image for Series
    /// </summary>
    /// <param name="series"></param>
    /// <param name="forceUpdate">Force updating cover image even if underlying file has not been modified or chapter already has a cover image</param>
    private Task UpdateSeriesCoverImage(Series? series, bool forceUpdate)
    {
        if (series == null) return Task.CompletedTask;

        if (!_cacheHelper.ShouldUpdateCoverImage(_directoryService.FileSystem.Path.Join(_directoryService.CoverImageDirectory, series.CoverImage),
                null, series.Created, forceUpdate, series.CoverImageLocked))
            return Task.CompletedTask;

        series.Volumes ??= [];

        var firstChapter = SeriesService.GetFirstChapterForMetadata(series);
        var firstFile = firstChapter?.Files.FirstOrDefault();
        

        string coverFilepath = Path.Join(Path.GetDirectoryName(firstFile.FilePath), "cover.jpg");
        string newCoverImage = "_s" + series.Id + ".jpg";
        if (File.Exists(coverFilepath) == false)
        {
            coverFilepath = Path.Join(Path.GetDirectoryName(firstFile.FilePath), "cover.png");
            newCoverImage = "_s" + series.Id + ".png";
            if (File.Exists(coverFilepath) == false)
            {
                coverFilepath = Path.Join(Path.GetDirectoryName(firstFile.FilePath), "cover.webp");
                newCoverImage = "_s" + series.Id + ".webp";
            }
        }
        string configCoverFilepath = Path.Join(_directoryService.CoverImageDirectory, newCoverImage);
        bool flagCover = false;
        if (File.Exists(coverFilepath) && !File.Exists(configCoverFilepath))
        {
            System.IO.File.Copy(coverFilepath, configCoverFilepath, true);
        }
        else if (File.Exists(coverFilepath) && File.Exists(configCoverFilepath) && (new FileInfo(coverFilepath).Length != new FileInfo(configCoverFilepath).Length))
        {
            System.IO.File.Copy(coverFilepath, configCoverFilepath, true);
        }
        if (!File.Exists(coverFilepath) && File.Exists(configCoverFilepath))
        {
            File.Delete(configCoverFilepath);
            flagCover = false;
        }
        if (File.Exists(configCoverFilepath))
        {
            flagCover = true;
            series.CoverImage = newCoverImage;
        }

        if (flagCover == false)
            series.CoverImage = series.GetCoverImage();

        _updateEvents.Add(MessageFactory.CoverUpdateEvent(series.Id, MessageFactoryEntityTypes.Series));
        return Task.CompletedTask;
    }


    /// <summary>
    ///
    /// </summary>
    /// <param name="series"></param>
    /// <param name="forceUpdate"></param>
    /// <param name="encodeFormat"></param>
    private async Task ProcessSeriesCoverGen(Series series, bool forceUpdate, EncodeFormat encodeFormat, CoverImageSize coverImageSize, GdsInfo gdsInfo)
    {
        _logger.LogDebug("[MetadataServiceGDS] Processing cover image generation for series: {SeriesName}", series.OriginalName);
        try
        {
            var volumeIndex = 0;
            var firstVolumeUpdated = false;
            foreach (var volume in series.Volumes)
            {
                var firstChapterUpdated = false; // This only needs to be FirstChapter updated
                var index = 0;
                foreach (var chapter in volume.Chapters)
                {
                    var chapterUpdated = await UpdateChapterCoverImage(chapter, forceUpdate, encodeFormat, coverImageSize, gdsInfo);
                    // If cover was update, either the file has changed or first scan and we should force a metadata update
                    UpdateChapterLastModified(chapter, forceUpdate || chapterUpdated);
                    if (index == 0 && chapterUpdated)
                    {
                        firstChapterUpdated = true;
                    }
                    index++;
                }

                var volumeUpdated = await UpdateVolumeCoverImage(volume, firstChapterUpdated || forceUpdate);
                if (volumeIndex == 0 && volumeUpdated)
                {
                    firstVolumeUpdated = true;
                }
                volumeIndex++;
            }

            var useFirstCover = false;
            if (series.Format == MangaFormat.Text) useFirstCover = true;
            if (gdsInfo != null && gdsInfo.action != null && gdsInfo.action.ContainsKey("first_cover") && gdsInfo.action["first_cover"] == "true")
            {
                useFirstCover = true;
            }
            if (useFirstCover == false)
            {
                if (Path.Exists(Path.Join(Path.GetDirectoryName(Path.GetDirectoryName(series.FolderPath)), ".firstcover")) || Path.Exists(Path.Join(Path.GetDirectoryName(series.FolderPath), ".firstcover")) || Path.Exists(Path.Join(series.FolderPath, ".firstcover")))
                {
                    useFirstCover = true;
                }
            }
            
            await UpdateSeriesCoverImage(series, firstVolumeUpdated || forceUpdate);

            if (useFirstCover)
            {
                var coverImage = series.CoverImage;
                if ( coverImage == null) 
                {
                    foreach (var volume in series.Volumes)
                    {
                        foreach (var chapter in volume.Chapters)
                        {
                            if (chapter.CoverImage != null)
                            {
                                coverImage = chapter.CoverImage;
                                break;
                            }
                        }
                    }
                }
                if (coverImage != null)
                {
                    //var firstVolume = series.Volumes.FirstNonLooseLeafOrDefault();
                    foreach (var volume in series.Volumes)
                    {
                        //volume.CoverImage = firstVolume.CoverImage;
                        volume.CoverImage = coverImage;
                        foreach (var chapter in volume.Chapters)
                        {
                            chapter.CoverImage = coverImage;
                        }
                    }
                    series.CoverImage = coverImage;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[MetadataServiceGDS] There was an exception during cover generation for {SeriesName} ", series.Name);
        }
    }


    /// <summary>
    /// Refreshes Cover Images for a whole library
    /// </summary>
    /// <remarks>This can be heavy on memory first run</remarks>
    /// <param name="libraryId"></param>
    /// <param name="forceUpdate">Force updating cover image even if underlying file has not been modified or chapter already has a cover image</param>
    [DisableConcurrentExecution(timeoutInSeconds: 60 * 60 * 60)]
    [AutomaticRetry(Attempts = 3, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
    public async Task GenerateCoversForLibrary(int libraryId, bool forceUpdate = false)
    {
        var library = await _unitOfWork.LibraryRepository.GetLibraryForIdAsync(libraryId);
        if (library == null) return;
        _logger.LogInformation("[MetadataServiceGDS] Beginning cover generation refresh of {LibraryName}", library.Name);

        _updateEvents.Clear();

        var chunkInfo = await _unitOfWork.SeriesRepository.GetChunkInfo(library.Id);
        var stopwatch = Stopwatch.StartNew();
        var totalTime = 0L;
        _logger.LogInformation("[MetadataServiceGDS] Refreshing Library {LibraryName} for cover generation. Total Items: {TotalSize}. Total Chunks: {TotalChunks} with {ChunkSize} size", library.Name, chunkInfo.TotalSize, chunkInfo.TotalChunks, chunkInfo.ChunkSize);

        await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
            MessageFactory.CoverUpdateProgressEvent(library.Id, 0F, ProgressEventType.Started, $"Starting {library.Name}"));

        var settings = await _unitOfWork.SettingsRepository.GetSettingsDtoAsync();
        var encodeFormat = settings.EncodeMediaAs;
        var coverImageSize = settings.CoverImageSize;

        for (var chunk = 1; chunk <= chunkInfo.TotalChunks; chunk++)
        {
            if (chunkInfo.TotalChunks == 0) continue;
            totalTime += stopwatch.ElapsedMilliseconds;
            stopwatch.Restart();

            _logger.LogDebug("[MetadataServiceGDS] Processing chunk {ChunkNumber} / {TotalChunks} with size {ChunkSize}. Series ({SeriesStart} - {SeriesEnd})",
                chunk, chunkInfo.TotalChunks, chunkInfo.ChunkSize, chunk * chunkInfo.ChunkSize, (chunk + 1) * chunkInfo.ChunkSize);

            var nonLibrarySeries = await _unitOfWork.SeriesRepository.GetFullSeriesForLibraryIdAsync(library.Id,
                new UserParams()
                {
                    PageNumber = chunk,
                    PageSize = chunkInfo.ChunkSize
                });
            _logger.LogDebug("[MetadataServiceGDS] Fetched {SeriesCount} series for refresh", nonLibrarySeries.Count);

            var seriesIndex = 0;
            foreach (var series in nonLibrarySeries)
            {
                var index = chunk * seriesIndex;
                var progress =  Math.Max(0F, Math.Min(1F, index * 1F / chunkInfo.TotalSize));

                await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
                    MessageFactory.CoverUpdateProgressEvent(library.Id, progress, ProgressEventType.Updated, series.Name));

                try
                {
                    GdsInfo gdsInfo = GdsUtil.getGdsInfoBySeries(series);
                    await ProcessSeriesCoverGen(series, forceUpdate, encodeFormat, coverImageSize, gdsInfo);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[MetadataServiceGDS] There was an exception during cover generation refresh for {SeriesName}", series.Name);
                }
                seriesIndex++;
            }

            await _unitOfWork.CommitAsync();

            await FlushEvents();

            _logger.LogInformation(
                "[MetadataServiceGDS] Processed {SeriesStart} - {SeriesEnd} out of {TotalSeries} series in {ElapsedScanTime} milliseconds for {LibraryName}",
                chunk * chunkInfo.ChunkSize, (chunk * chunkInfo.ChunkSize) + nonLibrarySeries.Count, chunkInfo.TotalSize, stopwatch.ElapsedMilliseconds, library.Name);
        }

        await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
            MessageFactory.CoverUpdateProgressEvent(library.Id, 1F, ProgressEventType.Ended, $"Complete"));

        _logger.LogInformation("[MetadataServiceGDS] Updated covers for {SeriesNumber} series in library {LibraryName} in {ElapsedMilliseconds} milliseconds total", chunkInfo.TotalSize, library.Name, totalTime);
    }


    public async Task RemoveAbandonedMetadataKeys()
    {
        await _unitOfWork.TagRepository.RemoveAllTagNoLongerAssociated();
        await _unitOfWork.PersonRepository.RemoveAllPeopleNoLongerAssociated();
        await _unitOfWork.GenreRepository.RemoveAllGenreNoLongerAssociated();
        await _unitOfWork.CollectionTagRepository.RemoveCollectionsWithoutSeries();
        await _unitOfWork.AppUserProgressRepository.CleanupAbandonedChapters();

    }

    /// <summary>
    /// Refreshes Metadata for a Series. Will always force updates.
    /// </summary>
    /// <param name="libraryId"></param>
    /// <param name="seriesId"></param>
    /// <param name="forceUpdate">Overrides any cache logic and forces execution</param>
    public async Task GenerateCoversForSeries(int libraryId, int seriesId, bool forceUpdate = true, GdsInfo gdsInfo=null)
    {
        var series = await _unitOfWork.SeriesRepository.GetFullSeriesForSeriesIdAsync(seriesId);
        if (series == null)
        {
            _logger.LogError("[MetadataServiceGDS] Series {SeriesId} was not found on Library {LibraryId}", seriesId, libraryId);
            return;
        }
        if (gdsInfo == null)
        {
            gdsInfo = GdsUtil.getGdsInfoBySeries(series);
        }
        var settings = await _unitOfWork.SettingsRepository.GetSettingsDtoAsync();
        var encodeFormat = settings.EncodeMediaAs;
        var coverImageSize = settings.CoverImageSize;
        await GenerateCoversForSeries(series, encodeFormat, coverImageSize, forceUpdate, gdsInfo);
    }

    /// <summary>
    /// Generate Cover for a Series. This is used by Scan Loop and should not be invoked directly via User Interaction.
    /// </summary>
    /// <param name="series">A full Series, with metadata, chapters, etc</param>
    /// <param name="encodeFormat">When saving the file, what encoding should be used</param>
    /// <param name="forceUpdate"></param>
    public async Task GenerateCoversForSeries(Series series, EncodeFormat encodeFormat, CoverImageSize coverImageSize, bool forceUpdate = false, GdsInfo gdsInfo=null)
    {
        var sw = Stopwatch.StartNew();
        await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
            MessageFactory.CoverUpdateProgressEvent(series.LibraryId, 0F, ProgressEventType.Started, series.Name));

        await ProcessSeriesCoverGen(series, forceUpdate, encodeFormat, coverImageSize, gdsInfo);


        if (_unitOfWork.HasChanges())
        {
            await _unitOfWork.CommitAsync();
            _logger.LogInformation("[MetadataServiceGDS] Updated covers for {SeriesName} in {ElapsedMilliseconds} milliseconds", series.Name, sw.ElapsedMilliseconds);
        }

        await _eventHub.SendMessageAsync(MessageFactory.NotificationProgress,
            MessageFactory.CoverUpdateProgressEvent(series.LibraryId, 1F, ProgressEventType.Ended, series.Name));

        await _eventHub.SendMessageAsync(MessageFactory.CoverUpdate, MessageFactory.CoverUpdateEvent(series.Id, MessageFactoryEntityTypes.Series), false);
        await FlushEvents();
    }

    private async Task FlushEvents()
    {
        // Send all events out now that entities are saved
        _logger.LogDebug("Dispatching {Count} update events", _updateEvents.Count);
        foreach (var updateEvent in _updateEvents)
        {
            await _eventHub.SendMessageAsync(MessageFactory.CoverUpdate, updateEvent, false);
        }
        _updateEvents.Clear();
    }


    
}



