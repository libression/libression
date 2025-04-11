import { useState } from "react";
import { apiService } from "../services/api";
import type { FileEntry } from "../../types";

interface ActionBarProps {
  selectedFiles: string[];
  currentPath: string;
  onRefresh: () => void;
  files: FileEntry[];
  setSelectedFiles: (files: string[]) => void;
}

export default function ActionBar({
  selectedFiles,
  currentPath,
  onRefresh,
  files,
  setSelectedFiles,
}: ActionBarProps) {
  const [targetDir, setTargetDir] = useState("");
  const [filesToUpload, setFilesToUpload] = useState<File[]>([]);

  const handleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;

    setFilesToUpload(Array.from(files));

    try {
      const response = await apiService.uploadFiles(
        Array.from(files),
        currentPath,
      );
      if (!response.error) {
        onRefresh();
        setFilesToUpload([]);
      } else {
        console.error("Upload failed:", response.error);
        setFilesToUpload([]);
      }
    } catch (error) {
      console.error("Error uploading files:", error);
      setFilesToUpload([]);
    }
  };

  const handleAction = async (action: "copy" | "move" | "delete") => {
    if (selectedFiles.length === 0) return;

    try {
      let response;
      if (action === "delete") {
        response = await apiService.deleteFiles(
          selectedFiles.map((file) => {
            const fileEntry = files.find((f) => f.file_key === file);
            if (!fileEntry) {
              throw new Error(`File entry not found for ${file}`);
            }
            return fileEntry;
          }),
        );
      } else {
        const fileMappings = selectedFiles.map((file) => {
          const fileEntry = files.find((f) => f.file_key === file);
          if (!fileEntry) {
            throw new Error(`File entry not found for ${file}`);
          }
          return {
            source_key: fileEntry.file_key,
            destination_key: `${targetDir}/${fileEntry.file_key.split("/").pop()}`,
          };
        });

        response = await apiService.copyFiles(fileMappings, action === "move");
      }

      if (!response.error) {
        onRefresh();
        setSelectedFiles([]);
      } else {
        console.error(`${action} action failed:`, response.error);
      }
    } catch (error) {
      console.error(`Error performing ${action} action:`, error);
    }
  };

  return (
    <div className="bg-gray-200 p-4 flex items-center justify-between">
      <div>
        <label className="bg-blue-500 text-white px-4 py-2 rounded mr-2 cursor-pointer">
          Upload Files
          <input 
            type="file" 
            multiple 
            onChange={handleUpload} 
            className="hidden" 
          />
        </label>
        {filesToUpload.length > 0 && (
          <span className="text-gray-800 ml-2">
            {filesToUpload.length} file{filesToUpload.length > 1 ? 's' : ''} to upload
          </span>
        )}
        <button
          onClick={() => handleAction("copy")}
          className="bg-blue-500 text-white px-4 py-2 rounded mr-2"
        >
          Copy
        </button>
        <button
          onClick={() => handleAction("move")}
          className="bg-green-500 text-white px-4 py-2 rounded mr-2"
        >
          Move
        </button>
        <button
          onClick={() => handleAction("delete")}
          className="bg-red-500 text-white px-4 py-2 rounded"
        >
          Delete
        </button>
      </div>
      <div className="flex items-center">
        <input
          type="text"
          value={targetDir}
          onChange={(e) => setTargetDir(e.target.value)}
          placeholder="Target directory"
          className="border rounded px-2 py-1 mr-2 text-black placeholder-gray-500"
        />
        <span className="text-gray-800">{selectedFiles.length} items selected</span>
      </div>
    </div>
  );
}
