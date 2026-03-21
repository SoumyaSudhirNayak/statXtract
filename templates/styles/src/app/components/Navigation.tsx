import { useNavigate, useLocation } from "react-router";
import { Home, ChevronRight } from "lucide-react";
import { useState } from "react";

interface NavigationProps {
  userRole?: "admin" | "user";
}

export function Navigation({ userRole }: NavigationProps) {
  const navigate = useNavigate();
  const location = useLocation();
  const [hoveredItem, setHoveredItem] = useState<number | null>(null);

  // Generate breadcrumbs from current path
  const generateBreadcrumbs = () => {
    const paths = location.pathname.split("/").filter(Boolean);
    const breadcrumbs = [];

    if (paths.length === 0) {
      return [{ label: "Home", path: "/" }];
    }

    // Add home/dashboard
    if (userRole === "admin") {
      breadcrumbs.push({ label: "Dashboard", path: "/admin/dashboard" });
    } else if (userRole === "user") {
      breadcrumbs.push({ label: "Dashboard", path: "/user/dashboard" });
    }

    // Format current page name
    const currentPage = paths[paths.length - 1];
    const formattedPage = currentPage
      .split("-")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");

    if (currentPage !== "dashboard") {
      breadcrumbs.push({
        label: formattedPage,
        path: location.pathname,
      });
    }

    return breadcrumbs;
  };

  const breadcrumbs = generateBreadcrumbs();

  return (
    <nav className="bg-white border-b-2 border-[#2E5BBA] shadow-md relative z-40">
      <div className="max-w-[1800px] mx-auto px-10 py-5">
        {/* Breadcrumbs */}
        <div className="flex items-center gap-4">
          <div className="bg-[#F5F7FA] p-2 rounded-lg border-2 border-[#D1D5DB]">
            <Home className="w-6 h-6 text-[#2E5BBA]" />
          </div>
          {breadcrumbs.map((crumb, index) => (
            <div key={index} className="flex items-center gap-4">
              {index > 0 && <ChevronRight className="w-6 h-6 text-[#D1D5DB]" />}
              <button
                onClick={() => navigate(crumb.path)}
                className={`text-xl transition-all duration-300 bg-transparent border-none cursor-pointer px-4 py-2 rounded-xl font-bold tracking-tight ${
                  index === breadcrumbs.length - 1
                    ? "text-[#2E5BBA] bg-[#2E5BBA]/5 border-2 border-[#2E5BBA]"
                    : "text-[#6B7280] hover:text-[#2E5BBA] hover:bg-[#F5F7FA] border-2 border-transparent"
                }`}
                onMouseEnter={() => setHoveredItem(index)}
                onMouseLeave={() => setHoveredItem(null)}
                style={{
                  boxShadow: hoveredItem === index ? '0 4px 10px rgba(46, 91, 186, 0.1)' : 'none'
                }}
              >
                {crumb.label}
              </button>
            </div>
          ))}
        </div>
      </div>
    </nav>
  );
}