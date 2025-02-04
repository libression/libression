export interface FileEntry {
  file_key: string;
  file_entity_uuid: string;
  thumbnail_key: string | null;
  thumbnail_mime_type: string | null;
  thumbnail_checksum: string | null;
  thumbnail_phash: string | null;
  mime_type: string | null;
  tags: string[];
}

export interface ListDirectoryObject {
  filename: string;
  absolute_path: string;
  size: number;
  modified: string;
  is_dir: boolean;
}
