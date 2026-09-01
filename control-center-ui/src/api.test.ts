import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("api response validation", () => {
  it("rejects an HTML success response before attempting JSON parsing", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("<!doctype html><html></html>", {
      status: 200, headers: { "Content-Type": "text/html; charset=utf-8" },
    })));

    await expect(api("/api/lane-iii/paper/slim-status")).rejects.toThrow("backend endpoint returned HTML");
  });

  it("uses the same concise response-type error for non-OK HTML", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("<!doctype html><html></html>", {
      status: 503, headers: { "Content-Type": "text/html" },
    })));

    await expect(api("/api/lane-iii/paper")).rejects.toThrow("backend endpoint returned HTML");
  });

  it("reports invalid JSON without exposing a parser exception", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("not-json", {
      status: 200, headers: { "Content-Type": "application/json" },
    })));

    await expect(api("/api/lane-iii/paper")).rejects.toThrow("backend endpoint returned invalid JSON");
  });
});
