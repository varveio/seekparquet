// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package io.varve.seekparquet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/** Strict manifest validation (spec §3). */
final class ManifestParser {

    private static final Set<String> TOP_MEMBERS =
            Set.of("format", "format_version", "draft_revision", "schema_id", "parts", "extensions");
    private static final Set<String> PART_MEMBERS =
            Set.of("path", "rows", "bytes", "min_key_base64", "max_key_base64", "sha256");
    private static final Pattern INTEGER = Pattern.compile("0|[1-9][0-9]*");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern BASE64 =
            Pattern.compile("(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})");

    private ManifestParser() {}

    private static RefusalException invalid(String detail) {
        return new RefusalException(RefusalCode.INVALID_MANIFEST, null, detail);
    }

    static Manifest parse(byte[] raw) {
        if (raw.length >= 3 && (raw[0] & 0xFF) == 0xEF && (raw[1] & 0xFF) == 0xBB && (raw[2] & 0xFF) == 0xBF) {
            throw invalid("the manifest begins with a byte order mark");
        }
        if (!Utf8.isValid(raw)) {
            throw invalid("the manifest is not valid UTF-8");
        }
        Json.Node root;
        try {
            root = Json.parse(raw);
        } catch (Json.SyntaxException e) {
            throw invalid(e.getMessage());
        }
        if (!(root instanceof Json.Obj top)) {
            throw invalid("the manifest is not a JSON object");
        }
        Map<String, Json.Node> m = top.members();

        // MAN-7: the version comes first.
        if (!(m.get("format") instanceof Json.Str format) || !format.value().equals("seekparquet")) {
            throw invalid("format is missing or not \"seekparquet\"");
        }
        Json.Node fv = m.get("format_version");
        if (fv == null) {
            throw invalid("format_version is missing");
        }
        long formatVersion = integer(fv, "format_version");
        Json.Node dr = m.get("draft_revision");
        OptionalLong draft = OptionalLong.empty();
        if (formatVersion == 0) {
            if (dr == null) {
                throw invalid("format_version 0 requires a draft_revision");
            }
            long d = integer(dr, "draft_revision");
            if (d < 1) {
                throw invalid("draft_revision is below 1");
            }
            draft = OptionalLong.of(d);
        } else if (dr != null) {
            throw invalid("draft_revision is present on format_version " + formatVersion);
        }
        FormatVersion version = new FormatVersion(formatVersion, draft);
        if (!version.isSupported()) {
            throw new RefusalException(RefusalCode.UNSUPPORTED_VERSION, null, "this reader does not support " + version);
        }

        // MAN-3: the rules of the supported version.
        for (String name : m.keySet()) {
            if (!TOP_MEMBERS.contains(name)) {
                throw invalid("undefined member \"" + name + "\"");
            }
        }
        if (!(m.get("schema_id") instanceof Json.Str schemaId)) {
            throw invalid("schema_id is missing or not a string");
        }
        if (schemaId.value().isEmpty() || hasControl(schemaId.value())) {
            throw invalid("schema_id is empty or holds a control character");
        }
        Map<String, String> extensions = new LinkedHashMap<>();
        Json.Node ext = m.get("extensions");
        if (ext != null) {
            if (!(ext instanceof Json.Obj extObj)) {
                throw invalid("extensions is not an object");
            }
            for (Map.Entry<String, Json.Node> e : extObj.members().entrySet()) {
                if (e.getKey().isEmpty()) {
                    throw invalid("an extension namespace is empty");
                }
                Json.Node v = e.getValue();
                extensions.put(e.getKey(), new String(raw, v.start(), v.end() - v.start(), StandardCharsets.UTF_8));
            }
        }
        Json.Node partsNode = m.get("parts");
        if (partsNode == null) {
            throw invalid("parts is missing");
        }
        if (!(partsNode instanceof Json.Arr partsArr)) {
            throw invalid("parts is not an array");
        }
        List<ManifestPart> parts = new ArrayList<>(partsArr.items().size());
        Map<String, Integer> paths = new HashMap<>();
        for (int i = 0; i < partsArr.items().size(); i++) {
            ManifestPart part = part(partsArr.items().get(i), i);
            Integer previous = paths.putIfAbsent(part.path(), i);
            if (previous != null) {
                throw invalid("parts " + previous + " and " + i + " have the same path \"" + part.path() + "\"");
            }
            parts.add(part);
        }
        // PART-4.
        for (int i = 0; i < parts.size(); i++) {
            ManifestPart p = parts.get(i);
            if (p.minKey().compareTo(p.maxKey()) > 0) {
                throw invalid("part " + i + " (" + p.path() + "): min_key is above max_key");
            }
            if (i > 0 && parts.get(i - 1).maxKey().compareTo(p.minKey()) >= 0) {
                throw invalid("part " + i + " (" + p.path() + ") does not start after part " + (i - 1) + " ends");
            }
        }
        return new Manifest(version, schemaId.value(), parts, extensions);
    }

    private static ManifestPart part(Json.Node node, int i) {
        String where = "part " + i;
        if (!(node instanceof Json.Obj obj)) {
            throw invalid(where + " is not an object");
        }
        Map<String, Json.Node> m = obj.members();
        for (String name : m.keySet()) {
            if (!PART_MEMBERS.contains(name)) {
                throw invalid(where + ": undefined member \"" + name + "\"");
            }
        }
        if (!(m.get("path") instanceof Json.Str path)) {
            throw invalid(where + ": path is missing or not a string");
        }
        checkPath(path.value(), where);
        long rows = requiredInteger(m, "rows", where);
        long bytes = requiredInteger(m, "bytes", where);
        if (rows < 1) {
            throw invalid(where + ": rows is below 1");
        }
        if (bytes < 1) {
            throw invalid(where + ": bytes is below 1");
        }
        Bytes min = key(m.get("min_key_base64"), where + ": min_key_base64");
        Bytes max = key(m.get("max_key_base64"), where + ": max_key_base64");
        Optional<String> sha = Optional.empty();
        Json.Node shaNode = m.get("sha256");
        if (shaNode != null) {
            if (!(shaNode instanceof Json.Str s) || !SHA256.matcher(s.value()).matches()) {
                throw invalid(where + ": sha256 is not 64 lowercase hexadecimal digits");
            }
            sha = Optional.of(s.value());
        }
        return new ManifestPart(path.value(), rows, bytes, min, max, sha);
    }

    private static long requiredInteger(Map<String, Json.Node> m, String name, String where) {
        Json.Node n = m.get(name);
        if (n == null) {
            throw invalid(where + ": " + name + " is missing");
        }
        return integer(n, where + ": " + name);
    }

    /** MAN-6: a JSON number with no sign, fraction, exponent or leading zero, at most 2^63-1. */
    private static long integer(Json.Node n, String what) {
        if (!(n instanceof Json.Num num) || !INTEGER.matcher(num.text()).matches()) {
            throw invalid(what + " is not a non-negative integer written without fraction or exponent");
        }
        try {
            return Long.parseLong(num.text());
        } catch (NumberFormatException e) {
            throw invalid(what + " is above 2^63 - 1");
        }
    }

    private static Bytes key(Json.Node n, String what) {
        if (!(n instanceof Json.Str s)) {
            throw invalid(what + " is missing or not a string");
        }
        if (!BASE64.matcher(s.value()).matches()) {
            throw invalid(what + " is not canonical padded base64 of a non-empty key");
        }
        try {
            return Bytes.fromBase64(s.value());
        } catch (IllegalArgumentException e) {
            throw invalid(what + ": " + e.getMessage());
        }
    }

    /** PART-1. */
    private static void checkPath(String path, String where) {
        if (path.isEmpty()) {
            throw invalid(where + ": path is empty");
        }
        if (path.startsWith("/")) {
            throw invalid(where + ": path is absolute");
        }
        if (hasControl(path) || path.indexOf('\\') >= 0) {
            throw invalid(where + ": path holds a control character or a backslash");
        }
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw invalid(where + ": path has an empty, \".\" or \"..\" segment");
            }
        }
    }

    private static boolean hasControl(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                return true;
            }
        }
        return false;
    }
}
