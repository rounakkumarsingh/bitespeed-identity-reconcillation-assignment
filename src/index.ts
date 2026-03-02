import { Hono } from "hono";
import { zValidator } from "@hono/zod-validator";
import { SQL, sql } from "bun";
import { z } from "zod";

/* ================================
   DB
================================ */
const db = new SQL("postgres://postgres@localhost:5432/postgres");

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
            phoneNumbers: [contact["phone_number"]],
            emails: [contact["email"]],
            secondaryContactIds: [],
        };
    }

    // BFS Search (edge exists iff either email or phone_number match)
    const queue = [...rows];
    const visited = new Set<number>();
    const emailIds = new Set<string>();
    const phoneNumbers = new Set<string>();
    let primaryContactId = null;
    while (queue.length > 0) {
        const curr = queue.shift()!;
        if (!curr || visited.has(curr.id)) continue;
        if (curr["link_precedence"] === "primary") {
            console.log("primary found");
            if (primaryContactId !== null) {
                throw new Error("primaryContactId is null");
            }
            primaryContactId = curr.id;
        }
        if (!curr.email) emailIds.add(curr.email);
        if (!curr["phone_number"]) phoneNumbers.add(curr["phone_number"]);
        const neighbours = await db`
        SELECT * FROM contact
        WHERE 
        ${curr.email !== null ? sql`email = ${curr.email}` : sql`FALSE`}
        OR 
        ${curr["phone_number"] !== null ? sql`phone_number = ${curr["phone_number"]}` : sql`FALSE`}`.values();

        for (const neighbour of neighbours) {
            if (!visited.has(neighbour.id)) queue.push(neighbour);
        }
    }

    if (primaryContactId === null) {
        throw new Error("primaryContactId is still null");
    }

    visited.delete(primaryContactId);
    return {
        primaryContactId,
        emails: Array.from(emailIds),
        phoneNumbers: Array.from(phoneNumbers),
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
        console.error(err);

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
