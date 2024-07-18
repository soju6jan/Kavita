using System.IO;
using System.Text;
using API.Data.Metadata;
using API.Entities.Enums;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Components.Forms;

namespace API.Services.Tasks.Scanner.Parser;
#nullable enable

/// <summary>
/// This is the basic parser for handling Manga/Comic/Book libraries. This was previously DefaultParser before splitting each parser
/// into their own classes.
/// </summary>
public class GdsParser(IDirectoryService directoryService, IDefaultParser imageParser) : DefaultParser(directoryService)
{
    public override ParserInfo? Parse(string filePath, string rootPath, string libraryRoot, LibraryType type, ComicInfo? comicInfo = null)
    {

        var fileName = directoryService.FileSystem.Path.GetFileNameWithoutExtension(filePath);

        if (type != LibraryType.Image && Parser.IsCoverImage(directoryService.FileSystem.Path.GetFileName(filePath))) return null;

        if (Parser.IsImage(filePath))
        {
            return imageParser.Parse(filePath, rootPath, libraryRoot, LibraryType.Image, comicInfo);
        }

        var ret = new ParserInfo()
        {
            Filename = Path.GetFileName(filePath),
            Format = Parser.ParseFormat(filePath),
            Title = Parser.RemoveExtensionIfSupported(fileName)!,
            FullFilePath = Parser.NormalizePath(filePath),
            Series = string.Empty,
            ComicInfo = comicInfo
        };

        var tmp = Path.GetFileName(Path.GetDirectoryName(filePath));
        tmp = Regex.Replace(tmp, @"\[.*?\]", "").Trim();
        tmp = Regex.Replace(tmp, @"\s-{1,2}$", "").Trim();
        tmp = Regex.Replace(tmp, @"\s~{1,2}$", "").Trim();
        ret.Series = tmp;

        //var rootFolderName = directoryService.FileSystem.DirectoryInfo.New(rootPath).Name;
        //ret.Series = CleanupRegex.Replace(rootFolderName, string.Empty);
        ret.Chapters = Parser.DefaultChapter;
        ret.Volumes = Parser.ParseVolume(fileName, type);

        ret.Edition = string.Empty;
        if (ret.Volumes == Parser.LooseLeafVolume)
        {
            ret.IsSpecial = true;
        } else
        {
            ret.IsSpecial = false;
        }
        if (Path.Exists(Path.Join(libraryRoot, ".special")) || Path.Exists(Path.Join(Path.GetDirectoryName(filePath), ".special")))
        {
            ret.IsSpecial = true;
            ret.Volumes = Parser.LooseLeafVolume;
        }
        //ret.Chapters = ret.Title;
        return ret.Series == string.Empty ? null : ret;
    }

    /// <summary>
    /// Applicable for everything but ComicVine and Image library types
    /// </summary>
    /// <param name="filePath"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    public override bool IsApplicable(string filePath, LibraryType type)
    {
        return type == LibraryType.GDS;
    }
}
