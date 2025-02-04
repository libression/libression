import { useState } from "react";
import type { ListDirectoryObject } from "../../types";

interface NavigationBarProps {
  currentPath: string;
  setCurrentPath: (path: string) => void;
  directories: ListDirectoryObject[];
}

export default function NavigationBar({
  currentPath,
  setCurrentPath,
  directories,
}: NavigationBarProps) {
  const [showSubdirs, setShowSubdirs] = useState(false);

  const pathParts = currentPath.split("/").filter(Boolean);

  return (
    <nav className="bg-gray-800 text-white p-4 flex items-center">
      <button onClick={() => setCurrentPath("")} className="mr-4">
        Home
      </button>
      {pathParts.map((part, index) => (
        <div key={index} className="flex items-center">
          <button
            onClick={() =>
              setCurrentPath(pathParts.slice(0, index + 1).join("/"))
            }
            className="hover:underline"
          >
            {part}
          </button>
          <span className="mx-2">/</span>
        </div>
      ))}
      <div className="relative ml-auto">
        <button
          onClick={() => setShowSubdirs(!showSubdirs)}
          className="bg-blue-500 hover:bg-blue-600 px-3 py-1 rounded"
        >
          Subdirectories
        </button>
        {showSubdirs && (
          <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg z-10">
            {directories.map((dir) => (
              <button
                key={dir.absolute_path}
                onClick={() => {
                  setCurrentPath(dir.absolute_path);
                  setShowSubdirs(false);
                }}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                {dir.filename}
              </button>
            ))}
          </div>
        )}
      </div>
    </nav>
  );
}
