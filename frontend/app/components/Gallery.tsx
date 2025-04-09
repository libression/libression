import { useState, useEffect } from "react";
import type { FileEntry, ListDirectoryObject } from "../../types";
import { apiService } from "../services/api";

// Fallback icon component for failed thumbnails
const BrokenImageIcon = () => (
  <div className="w-full h-full flex items-center justify-center bg-gray-100">
    <svg
      className="w-12 h-12 text-gray-400"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M2.25 15.75l5.159-5.159a2.25 2.25 0 013.182 0l5.159 5.159m-1.5-1.5l1.409-1.409a2.25 2.25 0 013.182 0l2.909 2.909m-18 3.75h16.5a1.5 1.5 0 001.5-1.5V6a1.5 1.5 0 00-1.5-1.5H3.75A1.5 1.5 0 002.25 6v12a1.5 1.5 0 001.5 1.5zm10.5-11.25h.008v.008h-.008V8.25zm.375 0a.375.375 0 11-.75 0 .375.375 0 01.75 0z"
      />
    </svg>
  </div>
);

interface GalleryProps {
  files: FileEntry[];
  directories: ListDirectoryObject[];
  showDirectories: boolean;
  thumbnailSize: number;
  selectedFiles: string[];
  setSelectedFiles: (files: string[]) => void;
  onFileClick: (file: FileEntry) => void;
  onDirectoryClick: (dir: ListDirectoryObject) => void;
}

export default function Gallery({
  files,
  directories,
  showDirectories,
  thumbnailSize,
  selectedFiles,
  setSelectedFiles,
  onFileClick,
  onDirectoryClick,
}: GalleryProps) {
  const [displayedFiles, setDisplayedFiles] = useState<FileEntry[]>([]);
  const [page, setPage] = useState(1);
  const [thumbnailUrls, setThumbnailUrls] = useState<Record<string, string>>(
    {},
  );

  useEffect(() => {
    // Remove decoding since file keys should be used as-is
    setDisplayedFiles(files.slice(0, page * 200));
  }, [files, page]);

  useEffect(() => {
    const loadThumbnailUrls = async () => {
      const urls: Record<string, string> = {};
      for (const file of displayedFiles) {
        console.log("Processing file:", {
          fileKey: file.file_key,
          thumbnailKey: file.thumbnail_key,
          mimeType: file.thumbnail_mime_type,
          phash: file.thumbnail_phash,
          isVideo: file.thumbnail_mime_type?.toLowerCase().includes("video"),
        });

        if (file.file_key && file.thumbnail_key) {
          try {
            console.log("Fetching thumbnail URL for:", file.thumbnail_key);
            const url = await apiService.getThumbnailUrl(file.thumbnail_key);
            console.log("Received thumbnail URL:", {
              thumbnailKey: file.thumbnail_key,
              url,
              success: url && url.trim().length > 0,
            });

            if (url && url.trim().length > 0) {
              urls[file.file_key] = url;
              console.log("Loaded thumbnail URL:", {
                fileKey: file.file_key,
                thumbnailKey: file.thumbnail_key,
                url,
                mimeType: file.thumbnail_mime_type,
                phash: file.thumbnail_phash,
                isVideo: file.thumbnail_mime_type
                  ?.toLowerCase()
                  .includes("video"),
              });
            }
          } catch (error) {
            console.error(
              `Error loading thumbnail for ${file.file_key}:`,
              error,
            );
          }
        } else {
          console.log("Missing thumbnail data:", {
            fileKey: file.file_key,
            thumbnailKey: file.thumbnail_key,
            mimeType: file.thumbnail_mime_type,
            phash: file.thumbnail_phash,
            isVideo: file.thumbnail_mime_type?.toLowerCase().includes("video"),
          });
        }
      }
      setThumbnailUrls((prev) => ({ ...prev, ...urls }));
    };
    loadThumbnailUrls();
  }, [displayedFiles]);

  const toggleFileSelection = (file: FileEntry) => {
    setSelectedFiles(
      selectedFiles.includes(file.file_key)
        ? selectedFiles.filter((f) => f !== file.file_key)
        : [...selectedFiles, file.file_key],
    );
  };

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: `repeat(auto-fill, minmax(${thumbnailSize}px, 1fr))`,
        gap: `${Math.max(thumbnailSize * 0.1, 8)}px`,
        padding: `${Math.max(thumbnailSize * 0.1, 8)}px`,
      }}
    >
      {displayedFiles.map((file) => {
        const decodedKey = decodeURIComponent(file.file_key);
        const thumbnailUrl =
          thumbnailUrls[decodedKey] || thumbnailUrls[file.file_key];

        return (
          <div
            key={decodedKey}
            className="relative group"
            style={{ width: thumbnailSize, height: thumbnailSize }}
          >
            <input
              type="checkbox"
              checked={selectedFiles.includes(file.file_key)}
              onChange={() => toggleFileSelection(file)}
              className="absolute top-2 left-2 z-10"
            />
            <div
              className="block w-full h-full cursor-pointer"
              onClick={async (e) => {
                if (
                  e.target instanceof HTMLInputElement &&
                  e.target.type === "checkbox"
                ) {
                  return;
                }
                onFileClick(file);
              }}
            >
              <div
                className="w-full h-full bg-cover bg-center"
                style={{
                  backgroundImage:
                    thumbnailUrl &&
                    file.thumbnail_phash &&
                    !file.thumbnail_mime_type?.toLowerCase().includes("video")
                      ? `url(${thumbnailUrl})`
                      : "none",
                }}
              >
                {(() => {
                  console.log("Rendering thumbnail:", {
                    fileKey: file.file_key,
                    thumbnailUrl,
                    mimeType: file.thumbnail_mime_type,
                    phash: file.thumbnail_phash,
                    hasUrl: !!thumbnailUrl,
                    hasPhash: !!file.thumbnail_phash,
                    isVideo: file.thumbnail_mime_type
                      ?.toLowerCase()
                      .includes("video"),
                  });

                  if (!thumbnailUrl) {
                    return <BrokenImageIcon />;
                  }

                  if (
                    file.thumbnail_mime_type?.toLowerCase().includes("video")
                  ) {
                    return (
                      <video
                        src={thumbnailUrl}
                        className="w-full h-full object-cover"
                        autoPlay
                        loop
                        muted
                        playsInline
                        onError={(e) => {
                          console.error("Video error:", {
                            fileKey: file.file_key,
                            thumbnailUrl,
                            mimeType: file.thumbnail_mime_type,
                            error: e,
                          });
                        }}
                      />
                    );
                  }

                  if (file.thumbnail_phash) {
                    return (
                      <img
                        src={thumbnailUrl}
                        className="w-full h-full object-cover"
                        alt=""
                        onError={(e) => {
                          console.error("Image error:", {
                            fileKey: file.file_key,
                            thumbnailUrl,
                            mimeType: file.thumbnail_mime_type,
                            error: e,
                          });
                        }}
                      />
                    );
                  }

                  return <BrokenImageIcon />;
                })()}
                {/* Filename overlay */}
                <div className="absolute bottom-0 left-0 right-0 p-2 bg-black bg-opacity-50 opacity-0 group-hover:opacity-100 transition-opacity duration-200">
                  <p className="text-white text-xs truncate">
                    {decodeURIComponent(file.file_key.split("/").pop() || "")}
                  </p>
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
