import { getCommandCenterData } from "@/lib/data"
import { CommandCenter } from "@/components/command-center"

// Snowflake is not reachable during the docker build — render at request time.
export const dynamic = "force-dynamic"

export default async function Home() {
  const data = await getCommandCenterData()
  return <CommandCenter data={data} />
}
