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
    // 1. TODO add the case where secondary contacts are created
    // 2. TODO add the case where primary become secondary
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
    console.log(rows);

    if (rows.length === 0) {
        const [contact] = await db`
        INSERT INTO contact (email, phone_number, link_precedence)
        VALUES (${email}, ${phoneNumber}, 'primary')
        RETURNING *
        `;
        console.log(contact);
        return {
            primaryContactId: contact.id,
            phoneNumbers: [contact.phone_number],
            emails: [contact.email],
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
        const curr = queue.shift()!;
        console.log(`${cnt}: ${curr}`);
        if (!curr || visited.has(curr[0])) continue;
        if (curr[4] === "primary") {
            console.log("primary found");
            if (primaryContactId !== null) {
                throw new Error("primaryContactId is null");
            }
            primaryContactId = curr[0];
        }
        if (!curr[2]) emailIds.add(curr[2]);
        if (!curr[1]) phoneNumbers.add(curr[1]);
        const neighbours = await db`
        SELECT * FROM contact
        WHERE 
        ${curr.email !== null ? sql`email = ${curr[2]}` : sql`FALSE`}
        OR 
        ${curr[1] !== null ? sql`phone_number = ${curr[1]}` : sql`FALSE`}`.values();

        visited.add(curr[0]);
        for (const neighbour of neighbours) {
            if (!visited.has(neighbour.id)) queue.push(neighbour);
        }

        cnt += 1;
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
        console.log("Starting");
        const contacts = await findContacts(data);
        console.log("endiing");
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
