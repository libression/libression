"use client";

import { useState, useEffect, useCallback } from "react";
import Gallery from "./components/Gallery";
import NavigationBar from "./components/NavigationBar";
import ActionBar from "./components/ActionBar";
import type { FileEntry, ListDirectoryObject } from "../types";
import { apiService } from "./services/api";

export default function App() {
  const [currentPath, setCurrentPath] = useState<string>("");
  const [files, setFiles] = useState<FileEntry[]>([]);
  const [directories, setDirectories] = useState<ListDirectoryObject[]>([]);
  const [selectedFiles, setSelectedFiles] = useState<string[]>([]);
  const [showDirectories, setShowDirectories] = useState<boolean>(false);
  const [thumbnailSize, setThumbnailSize] = useState<number>(150);

  const fetchDirectoryContents = useCallback(
    async (path: string) => {
      try {
        const response = await apiService.fetchDirectoryContents(
          path,
          showDirectories,
        );
        if (response.error) {
          throw new Error(response.error);
        }

        if (!response.data || !Array.isArray(response.data.dir_contents)) {
          console.error("Unexpected response structure:", response.data);
          throw new Error("Unexpected response structure");
        }

        const dirs = response.data.dir_contents.filter(
          (item: ListDirectoryObject) => item.is_dir,
        );
        const fileKeys = response.data.dir_contents
          .filter((item: ListDirectoryObject) => !item.is_dir)
          .map((item: ListDirectoryObject) => item.absolute_path);

        setDirectories(dirs);
        if (fileKeys.length > 0) {
          fetchFilesInfo(fileKeys);
        } else {
          setFiles([]);
        }
      } catch (error) {
        console.error("Error fetching directory contents:", error);
        setDirectories([]);
        setFiles([]);
      }
    },
    [showDirectories],
  );

  useEffect(() => {
    fetchDirectoryContents(currentPath);
  }, [currentPath, fetchDirectoryContents]);

  const fetchFilesInfo = async (fileKeys: string[]) => {
    try {
      const response = await apiService.fetchFilesInfo(fileKeys);
      if (response.error) {
        throw new Error(response.error);
      }
      setFiles(response.data.files || []);
    } catch (error) {
      console.error("Error fetching files info:", error);
      setFiles([]);
    }
  };

  const fetchThumbnailUrl = async (thumbnailKey: string) => {
    try {
      const response = await fetch(
        apiService.getApiUrl("thumbnailsUrls") + "/" + thumbnailKey,
        {
          method: "GET",
          headers: apiService.defaultHeaders,
        },
      );

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      return {
        ...data,
        url: apiService.transformWebDAVUrl(data.url),
      };
    } catch (error) {
      console.error("Error fetching thumbnail URL:", error);
      return null;
    }
  };

  const onFileClick = async (file: FileEntry) => {
    try {
      const url = await apiService.getFilesUrl(file.file_key);
      if (!url) return;

      const isHeic = file.file_key.toLowerCase().endsWith(".heic");
      const viewableMimeTypes = [
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/svg+xml",
        "application/pdf",
        "video/mp4",
        "video/webm",
        "audio/mpeg",
        "audio/wav",
      ];

      const link = document.createElement("a");
      link.href = url;

      if (
        !isHeic &&
        file.mime_type &&
        viewableMimeTypes.includes(file.mime_type)
      ) {
        link.target = "_blank";
        link.rel = "noopener noreferrer";
      } else {
        link.download = file.file_key.split("/").pop() || "download";
      }

      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error("Error opening file:", error);
    }
  };

  return (
    <div className="flex flex-col h-screen">
      <NavigationBar
        currentPath={currentPath}
        setCurrentPath={setCurrentPath}
        directories={directories}
      />
      <ActionBar
        selectedFiles={selectedFiles}
        currentPath={currentPath}
        onRefresh={() => fetchDirectoryContents(currentPath)}
        files={files}
      />
      <div className="flex items-center justify-between px-4 py-2 bg-gray-100">
        <div className="flex items-center">
          <input
            type="checkbox"
            checked={showDirectories}
            onChange={(e) => setShowDirectories(e.target.checked)}
            className="mr-2"
          />
          <label>Show Directories</label>
        </div>
        <input
          type="range"
          min="50"
          max="300"
          value={thumbnailSize}
          onChange={(e) => setThumbnailSize(Number(e.target.value))}
          className="w-48"
        />
      </div>
      <Gallery
        files={files}
        directories={directories}
        showDirectories={showDirectories}
        thumbnailSize={thumbnailSize}
        selectedFiles={selectedFiles}
        setSelectedFiles={setSelectedFiles}
        onFileClick={onFileClick}
        onDirectoryClick={(dir) => setCurrentPath(dir.absolute_path)}
      />
    </div>
  );
}
