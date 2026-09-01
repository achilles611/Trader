const isJsonContentType = (contentType: string | null) => {
  const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase() || "";
  return mediaType === "application/json" || mediaType.endsWith("+json");
};

const responseTypeMessage = (contentType: string | null) => {
  const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase() || "";
  if (mediaType === "text/html") return "backend endpoint returned HTML";
  return mediaType ? `backend endpoint returned ${mediaType}` : "backend endpoint returned no content type";
};

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { headers: { "Content-Type": "application/json", ...(init?.headers || {}) }, ...init });
  const contentType = response.headers.get("Content-Type");
  if (!isJsonContentType(contentType)) throw new Error(responseTypeMessage(contentType));

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new Error("backend endpoint returned invalid JSON");
  }
  if (!response.ok) {
    const detail = body && typeof body === "object" && "detail" in body ? (body as { detail?: unknown }).detail : null;
    throw new Error(typeof detail === "string" ? detail : `Request failed (${response.status}). No state change was made.`);
  }
  return body as T;
}

export function post<T>(path: string, body?: unknown): Promise<T> {
  return api<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) });
}
