import Link from 'next/link';
import { notFound } from 'next/navigation';
import { Badge, SiteShell } from '@/components/Shell';
import { docPages, getAllDocParams, getDoc, getGroupedPages } from '@/lib/docs-data';
import { docsVersions } from '@/lib/versions';

export default async function DocPage({params}:{params:Promise<{version:string;slug?:string[]}>}){
  const {version,slug}=await params;
  const page=getDoc(version,slug);
  if(!page)notFound();
  const idx=docPages.findIndex(p=>p.slug===page.slug);
  const prev=docPages[idx-1];
  const next=docPages[idx+1];
  const groups=getGroupedPages();
  return (
    <SiteShell>
      <main className="container docs-layout">
        <aside className="sidebar">
          <select className="version" defaultValue={version} aria-label="Documentation version">
            {docsVersions.map(v=><option key={v.slug} value={v.slug}>{v.label}</option>)}
          </select>
          {groups.map(({group,pages})=>(
            <div key={group}>
              <div className="sidebar-group-label">{group}</div>
              {pages.map(p=>(
                <Link
                  key={p.slug}
                  href={`/docs/${version}${p.slug?`/${p.slug}`:''}`}
                  className={p.slug===page.slug?'sidebar-active':''}
                >
                  {p.title}
                </Link>
              ))}
            </div>
          ))}
        </aside>
        <article className="docs-main">
          <Badge tone={page.status==='Available'?'green':page.status==='Experimental'?'violet':page.status==='Preview'?'blue':'blue'}>
            {page.status}
          </Badge>
          <h1>{page.title}</h1>
          <p className="lead">{page.description}</p>
          <div className="prose" dangerouslySetInnerHTML={{__html: page.body}}/>
          <div className="prevnext">
            {prev?<Link className="btn btn-secondary" href={`/docs/${version}${prev.slug?`/${prev.slug}`:''}`}>← {prev.title}</Link>:<span/>}
            {next?<Link className="btn btn-secondary" href={`/docs/${version}/${next.slug}`}>{next.title} →</Link>:<span/>}
          </div>
        </article>
        <aside className="toc">
          <strong>On this page</strong>
          <a href="#overview">Overview</a>
          {page.body.includes('<h2>')&&page.body.match(/<h2[^>]*>([^<]+)<\/h2>/g)?.map(h=>{
            const text=h.replace(/<[^>]+>/g,'');
            const id=text.toLowerCase().replace(/[^a-z0-9]+/g,'-');
            return <a key={id} href={`#${id}`}>{text}</a>;
          })}
        </aside>
      </main>
    </SiteShell>
  );
}

export function generateStaticParams(){return getAllDocParams()}
export async function generateMetadata({params}:{params:Promise<{version:string;slug?:string[]}>}){
  const {version,slug}=await params;
  const p=getDoc(version,slug);
  return p?{title:p.title,description:p.description}:{};
}
