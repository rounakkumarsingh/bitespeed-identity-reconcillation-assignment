import { zValidator } from "@hono/zod-validator";
import { SQL, sql } from "bun";
import { Hono } from "hono";
import { z } from "zod";

/* ================================
    DB
=============================== */
const db = new SQL(process.env.NEON_CONNECTION_URL!);

await db`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`;
await db`CREATE TYPE linked_precedence AS ENUM ('primary', 'secondary')`;
await db`
CREATE TABLE IF NOT EXISTS contact (
    id SERIAL PRIMARY KEY,
    phone_number VARCHAR,
    email VARCHAR,
    linked_id INT,
    link_precedence linked_precedence NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ DEFAULT NULL
)`;

/* ================================
   App
================================ */
const app = new Hono();

/* ================================
   Validation
================================ */
const inputSchema = z
    .object({
        phoneNumber: z.string().optional(),
        email: z.email().optional(),
    })
    .refine(
        (data) => data.phoneNumber || data.email,
        "Either phoneNumber or email is required",
    );

type IdentifyInput = z.infer<typeof inputSchema>;

const outputSchema = z.object({
    primaryContactId: z.number(),
    emails: z.array(z.email()),
    phoneNumbers: z.array(z.string()),
    secondaryContactIds: z.array(z.number()),
});

type ContactOutput = z.infer<typeof outputSchema>;
/* ================================
   Data Layer
================================ */
async function findContacts({
    phoneNumber,
    email,
}: IdentifyInput): Promise<ContactOutput> {
    // 1. TODO add the case where primary become secondary
    if (!phoneNumber && !email) {
        throw new Error("Invalid data format");
    }

    const emailFilter =
        email !== undefined ? sql`email = ${email}` : sql`FALSE`;

    const phoneFilter =
        phoneNumber !== undefined
            ? sql`phone_number = ${phoneNumber}`
            : sql`FALSE`;

    const rows = await db`
    SELECT *
    FROM contact
    WHERE ${emailFilter} OR ${phoneFilter}
  `.values();


    if (rows.length === 0) {
        const [contact] = await db`
        INSERT INTO contact (email, phone_number, link_precedence)
        VALUES (${email}, ${phoneNumber}, 'primary')
        RETURNING *
        `;

        return {
            primaryContactId: contact.id,
            phoneNumbers: contact.phone_number ? [contact.phone_number] : [],
            emails: contact.email ? [contact.email] : [],
            secondaryContactIds: [],
        };
    }

    // BFS Search (edge exists iff either email or phone_number match)
    const queue = [...rows];
    const visited = new Set<number>();
    const emailIds = new Set<string>();
    const phoneNumbers = new Set<string>();
    let primaryContactId = null;
    let cnt = 0;
    while (queue.length > 0) {
        const curr = queue.shift();
        if (!curr) {
            throw new Error("curr is empty");
        }
        if (visited.has(curr[0])) continue;
        if (curr[2]) emailIds.add(curr[2]);
        if (curr[1]) phoneNumbers.add(curr[1]);
        if (primaryContactId === null) {
            if (curr[4] === "primary") primaryContactId = curr[0];
            else primaryContactId = curr[2];
        } else {
            if (curr[4] === "primary") {
                await db`
                UPDATE contact
                SET linked_id = ${primaryContactId}, link_precedence = 'secondary'
                WHERE id = ${curr[0]}
                `;
                await db`
                UPDATE contact
                SET linked_id = ${primaryContactId}
                WHERE linked_id = ${curr[0]}
                `;
            }
        }
        const neighbours = await db`
        SELECT * FROM contact
        WHERE (
        ${curr[2] !== null ? sql`email = ${curr[2]}` : sql`FALSE`}
        OR 
        ${curr[1] !== null ? sql`phone_number = ${curr[1]}` : sql`FALSE`})
        AND id != ${curr[0]}`.values();

        visited.add(curr[0]);
        for (const neighbour of neighbours) {
            if (!visited.has(neighbour[0])) queue.push(neighbour);
        }

        cnt += 1;
    }

    if (primaryContactId === null) {
        throw new Error("primaryContactId is still null");
    }

    if (
        (email ? !emailIds.has(email) : false) ||
        (phoneNumber ? !phoneNumbers.has(phoneNumber) : false)
    ) {
        const [contact] = await db`
        INSERT INTO contact (email, phone_number, link_precedence, linked_id)
        VALUES (${email}, ${phoneNumber}, 'secondary', ${primaryContactId})
        RETURNING *
        `;

        visited.add(contact.id);
        email && emailIds.add(email);
        phoneNumber && phoneNumbers.add(phoneNumber);
    }

    visited.delete(primaryContactId);
    return {
        primaryContactId,
        emails: Array.from(emailIds).filter(Boolean),
        phoneNumbers: Array.from(phoneNumbers).filter(Boolean),
        secondaryContactIds: Array.from(visited),
    };
}
/* ================================
   Routes
================================ */
app.post("/identify", zValidator("json", inputSchema), async (c) => {
    try {
        const data = c.req.valid("json");
    const contacts = await findContacts(data);
        return c.json({
            success: true,
            data: contacts,
        });
    } catch (err) {

        return c.json(
            {
                success: false,
                error: "Internal Server Error",
            },
            500,
        );
    }
});

export default app;
