export async function convertFromStream(
  content: string | Blob | NodeJS.ReadableStream
): Promise<string> {
  if (typeof content === 'string') {
    return content;
  }

  if (!(content instanceof Blob)) {
    throw new Error('Type must be Blob');
  }

  const arrayBuffer = new Promise<ArrayBuffer>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as ArrayBuffer);
    reader.onerror = reject;
    reader.readAsArrayBuffer(content);
  });
  return Buffer.from(await arrayBuffer).toString();
}
