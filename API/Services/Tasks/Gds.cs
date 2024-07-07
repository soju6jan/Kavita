using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using API.Data;
using API.Data.Repositories;
using API.Entities;
using API.Entities.Enums;
using API.Extensions;
using API.Helpers;
using API.Services.Tasks.Metadata;
using API.Services.Tasks.Scanner.Parser;
using API.SignalR;
using Hangfire;
using Microsoft.Extensions.Logging;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using System.Text;
using Serilog;
using ILogger = Serilog.Core.Logger;

namespace API.Services.Tasks;
#nullable enable

public class GdsInfo
{
    //public IDictionary<string, IDictionary<string, string>> files;
    public IDictionary<string, string> action;
    public IDictionary<string, GdsFile> files;
    public IDictionary<string, string> meta;
    public List<IDictionary<string, string>> search;
}

public class GdsFile
{
    public string cover;
    public int page;
    public int wordcount;
    public IDictionary<string, string> meta;
}


public class GdsUtil
{
    public static readonly ILogger gdsLog = new LoggerConfiguration()
        .MinimumLevel.Debug()
        //.WriteTo.File(Path.Join(Directory.GetCurrentDirectory(), "config/logs/", "gds.log"), rollingInterval: RollingInterval.Day)
        .CreateLogger();

    public static GdsInfo getGdsInfo(string filePath)
    {
        try
        {
            var lines = File.ReadAllLines(filePath, Encoding.UTF8);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var ret = deserializer.Deserialize<GdsInfo>(string.Join("\n", lines));
            return ret;
        }
        catch (Exception ex)
        {
            gdsLog.Error("에러: {ex}", ex);
            return null;
        }
    }

    public static GdsInfo getGdsInfoBySeries(API.Entities.Series series)
    {
        return GdsUtil.getGdsInfo(Path.Join(series.FolderPath, "kavita.yaml"));
    }

    public static GdsInfo getGdsInfoByFile(string filePath)
    {
        return GdsUtil.getGdsInfo(Path.Join(Path.GetDirectoryName(filePath), "kavita.yaml"));
    }

    public static bool saveCover(string filepath, string cover)
    {
        if (cover == "") return false;
        try
        {
            File.WriteAllBytes(filepath, Convert.FromBase64String(cover));
            return true;
        }
        catch (Exception ex)
        {
            return false;
        }
    }

    public static GdsFile GetGdsFile(GdsInfo gdsInfo, string key)
    {
        if (gdsInfo == null) return null;
        if (gdsInfo.files.Count == 0) return null;
        if (gdsInfo.files.ContainsKey(key)) {
            return gdsInfo.files[key];
        }
        key = key.Normalize(System.Text.NormalizationForm.FormKD);
        if (gdsInfo.files.ContainsKey(key)) {
            return gdsInfo.files[key];
        }
        return null;
    }
}
