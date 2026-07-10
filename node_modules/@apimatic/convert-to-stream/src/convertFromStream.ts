import { convertFromStream as convertFromBlob } from './convertFromBlob';

export async function convertFromStream(
  content: string | Blob | NodeJS.ReadableStream
): Promise<string> {
  if (typeof content === 'string') {
    return content;
  }
  if (content instanceof Blob) {
    return convertFromBlob(content);
  }

  const chunks: Uint8Array[] = [];
  for await (const chunk of content) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString();
}
