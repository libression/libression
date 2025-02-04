import { getApiUrl, transformWebDAVUrl } from "../config/api";
import type { FileEntry, ListDirectoryObject } from "../../types";

interface ApiResponse<T> {
  data: T;
  error?: string;
}

const defaultHeaders = {
  "Content-Type": "application/json",
  Accept: "application/json",
};

export const apiService = {
  getApiUrl,
  transformWebDAVUrl,
  defaultHeaders,

  async getThumbnailUrl(fileKey: string | null): Promise<string> {
    if (!fileKey) return "";
    try {
      const response = await fetch(this.getApiUrl("thumbnailsUrls"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({ file_keys: [fileKey] }),
      });

      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);
      const data = await response.json();
      return this.transformWebDAVUrl(`${data.base_url}/${data.paths[fileKey]}`);
    } catch (error) {
      console.error("Error fetching thumbnail URL:", error);
      return "";
    }
  },

  async fetchDirectoryContents(
    path: string,
    showDirectories: boolean = false,
  ): Promise<ApiResponse<{ dir_contents: ListDirectoryObject[] }>> {
    try {
      const response = await fetch(getApiUrl("showDirContents"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({
          dir_key: path,
          subfolder_contents: showDirectories,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      return { data };
    } catch (error: unknown) {
      if (error instanceof Error) {
        return { data: { dir_contents: [] }, error: error.message };
      }
      return { data: { dir_contents: [] }, error: "An unknown error occurred" };
    }
  },

  async fetchFilesInfo(
    fileKeys: string[],
  ): Promise<ApiResponse<{ files: FileEntry[] }>> {
    try {
      const response = await fetch(getApiUrl("filesInfo"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({ file_keys: fileKeys }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      return { data };
    } catch (error: unknown) {
      if (error instanceof Error) {
        return { data: { files: [] }, error: error.message };
      }
      return { data: { files: [] }, error: "An unknown error occurred" };
    }
  },

  async uploadFiles(
    files: File[],
    targetDir: string,
  ): Promise<ApiResponse<{ files: FileEntry[] }>> {
    try {
      const uploadEntries = await Promise.all(
        Array.from(files).map(async (file) => ({
          file_source: await this.readFileAsBase64(file),
          filename: file.name,
        })),
      );

      const response = await fetch(getApiUrl("upload"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({
          files: uploadEntries,
          target_dir: targetDir,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      return { data };
    } catch (error: unknown) {
      if (error instanceof Error) {
        return { data: { files: [] }, error: error.message };
      }
      return { data: { files: [] }, error: "An unknown error occurred" };
    }
  },

  // Helper function to read file as base64
  async readFileAsBase64(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        if (typeof reader.result === "string") {
          // Remove the data URL prefix (e.g., "data:image/jpeg;base64,")
          const base64 = reader.result.split(",")[1];
          resolve(base64);
        } else {
          reject(new Error("Failed to read file as base64"));
        }
      };
      reader.onerror = () => reject(reader.error);
      reader.readAsDataURL(file);
    });
  },

  async copyFiles(
    fileMappings: { source_key: string; destination_key: string }[],
    deleteSource: boolean = false,
  ): Promise<ApiResponse<void>> {
    try {
      const response = await fetch(getApiUrl("copy"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({
          file_mappings: fileMappings,
          delete_source: deleteSource,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      return { data: undefined };
    } catch (error: unknown) {
      if (error instanceof Error) {
        return { data: undefined, error: error.message };
      }
      return { data: undefined, error: "An unknown error occurred" };
    }
  },

  async deleteFiles(
    fileEntries: { file_key: string }[],
  ): Promise<ApiResponse<void>> {
    try {
      const response = await fetch(getApiUrl("delete"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({ file_entries: fileEntries }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      return { data: undefined };
    } catch (error: unknown) {
      if (error instanceof Error) {
        return { data: undefined, error: error.message };
      }
      return { data: undefined, error: "An unknown error occurred" };
    }
  },

  async getFilesUrl(fileKey: string): Promise<string> {
    if (!fileKey) return "";
    try {
      const response = await fetch(this.getApiUrl("filesUrls"), {
        method: "POST",
        headers: defaultHeaders,
        body: JSON.stringify({ file_keys: [fileKey] }),
      });

      if (!response.ok)
        throw new Error(`HTTP error! status: ${response.status}`);
      const data = await response.json();
      return this.transformWebDAVUrl(`${data.base_url}/${data.paths[fileKey]}`);
    } catch (error) {
      console.error("Error fetching file URL:", error);
      return "";
    }
  },
};
